/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.clients.docker;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class SendFilesToIngest {
    private SendFilesToIngest() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <files>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        List<Path> filePaths = Stream.of(args).skip(2)
                .map(Path::of)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties properties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            uploadFilesAndSendJob(properties, tableName, filePaths, s3Client, sqsClient);
        } finally {
            s3Client.shutdown();
        }
    }

    public static void uploadFilesAndSendJob(
            InstanceProperties properties, String tableName, List<Path> filePaths, AmazonS3 s3Client, SqsClient sqsClient) {
        uploadFiles(properties, filePaths, s3Client);
        sendJobForFiles(properties, tableName, filePaths, sqsClient);
    }

    public static void uploadFiles(InstanceProperties properties, List<Path> filePaths, AmazonS3 s3Client) {
        filePaths.forEach(filePath -> s3Client.putObject(properties.get(DATA_BUCKET),
                "ingest/" + filePath.getFileName().toString(), filePath.toFile()));
    }

    public static void sendJobForFiles(InstanceProperties properties, String tableName, List<Path> filePaths, SqsClient sqsClient) {
        IngestJob job = IngestJob.builder()
                .files(filePaths.stream()
                        .map(filePath -> properties.get(DATA_BUCKET) + "/ingest/" + filePath.getFileName().toString())
                        .collect(Collectors.toList()))
                .tableName(tableName)
                .build();
        sqsClient.sendMessage(request -> request
                .queueUrl(properties.get(INGEST_JOB_QUEUE_URL))
                .messageBody(new IngestJobSerDe().toJson(job)));
    }
}
