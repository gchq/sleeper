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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.nio.file.Path;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class SendIngestJobForDirectory {
    private SendIngestJobForDirectory() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <instance-id> <source-directory>");
        }
        String instanceId = args[0];
        Path sourceDir = Path.of(args[1]);

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3GivenInstanceId(s3Client, instanceId);

        IngestJob job = IngestJob.builder()
                .files(sourceDir.toString())
                .tableName("system-test")
                .build();
        sqsClient.sendMessage(properties.get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(job));
    }
}
