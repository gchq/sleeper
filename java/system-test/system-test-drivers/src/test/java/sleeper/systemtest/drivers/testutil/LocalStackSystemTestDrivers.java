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
package sleeper.systemtest.drivers.testutil;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.systemtest.drivers.util.AwsSystemTestDrivers;
import sleeper.systemtest.drivers.util.SystemTestClients;

import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.configure;

public class LocalStackSystemTestDrivers extends AwsSystemTestDrivers {

    public LocalStackSystemTestDrivers(LocalStackContainer localStackContainer) {
        super(SystemTestClients.builder()
                .regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .s3(buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard()))
                .s3V2(buildAwsV2Client(localStackContainer, Service.S3, S3Client.builder()))
                .dynamoDB(buildAwsV1Client(localStackContainer, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard()))
                .sqs(buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard()))
                .configureHadoopSetter(conf -> configure(conf, localStackContainer))
                .skipAssumeRole(true)
                .build());
    }

}
