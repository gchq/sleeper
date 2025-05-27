/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.clients.api.IngestBatcherSender;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;

import java.util.List;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;

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
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            if (!tableIndex.getTableByName(tableName).isPresent()) {
                System.out.println("Table " + tableName + " not found in instance " + instanceId);
                System.exit(1);
            }
            IngestBatcherSender.toSqs(instanceProperties, sqsClient)
                    .submit(new IngestBatcherSubmitRequest(tableName, files));
        }
    }

}
