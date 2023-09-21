/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class SendFilesToIngest {
    private SendFilesToIngest() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: <instance-id> <files>");
        }
        String instanceId = args[0];
        List<Path> filePaths = Stream.of(args).skip(1)
                .map(Path::of)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3GivenInstanceId(s3Client, instanceId);
        uploadFilesAndSendJob(properties, filePaths, s3Client, sqsClient);
    }

    public static void uploadFilesAndSendJob(
            InstanceProperties properties, List<Path> filePaths, AmazonS3 s3Client, AmazonSQS sqsClient) {
        uploadFiles(properties, filePaths, s3Client);
        sendJobForFiles(properties, filePaths, sqsClient);
    }

    public static void uploadFiles(InstanceProperties properties, List<Path> filePaths, AmazonS3 s3Client) {
        filePaths.forEach(filePath -> s3Client.putObject(properties.get(INGEST_SOURCE_BUCKET),
                "ingest/" + filePath.getFileName().toString(), filePath.toFile()));
    }

    public static void sendJobForFiles(InstanceProperties properties, List<Path> filePaths, AmazonSQS sqsClient) {
        IngestJob job = IngestJob.builder()
                .files(filePaths.stream()
                        .map(filePath -> properties.get(INGEST_SOURCE_BUCKET) + "/ingest/" + filePath.getFileName().toString())
                        .collect(Collectors.toList()))
                .tableName("system-test")
                .build();
        sqsClient.sendMessage(properties.get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(job));
    }
}
