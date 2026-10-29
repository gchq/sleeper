/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.clients.table;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.api.IngestBatcherSender;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.configuration.utils.S3ExpandDirectories;
import sleeper.configuration.utils.S3ExpandDirectoriesResult;
import sleeper.configuration.utils.S3FileDetails;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;

import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.util.NumberFormatUtils.formatBytesAsHumanReadableString;

/**
 * Sends files in S3 to the ingest batcher, after checking that every path matches at least one file.
 */
public class SendToIngestBatcher {

    private SendToIngestBatcher() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: <instanceId> <tableName> <file1> [<file2> ...]");
            System.exit(1);
        }
        String instanceId = args[0];
        String tableName = args[1];
        List<String> files = List.of(args).subList(2, args.length);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            if (!tableIndex.getTableByName(tableName).isPresent()) {
                System.out.println("Table " + tableName + " not found in instance " + instanceId);
                System.exit(1);
            }
            S3ExpandDirectoriesResult result = new S3ExpandDirectories(s3Client).expandPaths(files);
            IngestBatcherSender sender = IngestBatcherSender.toSqs(instanceProperties, sqsClient);
            boolean sent = send(tableName, result, sender, ConsoleOutput.stdOut());
            if (!sent) {
                System.exit(1);
            }
        }
    }

    /**
     * Reports the files found in S3 and sends them to the ingest batcher. Sends nothing if any path matched no files.
     *
     * @param  tableName the name of the table to ingest into
     * @param  result    the files found at each path requested
     * @param  sender    the sender to the ingest batcher
     * @param  out       the console output
     * @return           true if the files were sent
     */
    static boolean send(String tableName, S3ExpandDirectoriesResult result,
            IngestBatcherSender sender, ConsoleOutput out) {
        List<S3FileDetails> foundFiles = result.streamFiles().toList();
        long totalBytes = foundFiles.stream().mapToLong(S3FileDetails::fileSizeBytes).sum();
        out.println("Files found:");
        foundFiles.forEach(file -> out.println(
                "  " + file.pathForJob() + " (" + formatBytesAsHumanReadableString(file.fileSizeBytes()) + ")"));
        out.println("Number of files found: " + foundFiles.size());
        out.println("Total size: " + formatBytesAsHumanReadableString(totalBytes));
        List<String> missingPaths = result.listMissingPaths();
        if (!missingPaths.isEmpty()) {
            out.println();
            out.println("Paths not found:");
            missingPaths.forEach(path -> out.println("  " + path));
            out.println();
            out.println("Nothing was sent to the ingest batcher, including the files found.");
            out.println("Fix or remove the missing paths and try again.");
            return false;
        }
        sender.submit(new IngestBatcherSubmitRequest(tableName, result.listJobPaths()));
        out.println();
        out.println("Sent to ingest batcher for table " + tableName);
        return true;
    }

}
