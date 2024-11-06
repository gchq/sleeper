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
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.systemtest.drivers.util.AwsSystemTestDrivers;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.runner.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.configureHadoop;

public class LocalStackSystemTestDrivers extends AwsSystemTestDrivers {
    private final SystemTestClients clients;

    private LocalStackSystemTestDrivers(SystemTestClients clients) {
        super(clients);
        this.clients = clients;
    }

    public static LocalStackSystemTestDrivers fromContainer(LocalStackContainer localStackContainer) {
        return new LocalStackSystemTestDrivers(SystemTestClients.builder()
                .regionProvider(() -> Region.of(localStackContainer.getRegion()))
                .s3(buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard()))
                .s3V2(buildAwsV2Client(localStackContainer, Service.S3, S3Client.builder()))
                .s3Async(buildAwsV2Client(localStackContainer, Service.S3, S3AsyncClient.builder()))
                .dynamoDB(buildAwsV1Client(localStackContainer, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard()))
                .sqs(buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard()))
                .sqsV2(buildAwsV2Client(localStackContainer, Service.SQS, SqsClient.builder()))
                .configureHadoopSetter(conf -> configureHadoop(conf, localStackContainer))
                .skipAssumeRole(true)
                .build());
    }

    @Override
    public SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters) {
        return new LocalStackSystemTestDeploymentDriver(parameters, clients);
    }

    @Override
    public SleeperInstanceDriver instance(SystemTestParameters parameters) {
        return new LocalStackSleeperInstanceDriver(parameters, clients);
    }

    @Override
    public SnapshotsDriver snapshots() {
        return new LocalStackSnapshotsDriver();
    }

    @Override
    public PollWithRetriesDriver pollWithRetries() {
        return PollWithRetriesDriver.noWaits();
    }

    public SystemTestClients clients() {
        return clients;
    }
}
