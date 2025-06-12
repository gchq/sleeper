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
package sleeper.localstack.test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.localstack.test.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.localstack.test.LocalStackHadoopConfigurationProvider.getHadoopConfiguration;

/**
 * Constants for shared AWS clients pointing to a LocalStack test container.
 */
public class SleeperLocalStackClients {

    private SleeperLocalStackClients() {
    }

    private static final LocalStackContainer CONTAINER = SleeperLocalStackContainer.INSTANCE;
    public static final AmazonS3 S3_CLIENT = buildAwsV1Client(CONTAINER, Service.S3, AmazonS3ClientBuilder.standard());
    public static final S3Client S3_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.S3, S3Client.builder());
    public static final S3AsyncClient S3_ASYNC_CLIENT = buildAwsV2Client(CONTAINER, Service.S3, S3AsyncClient.crtBuilder());
    public static final S3TransferManager S3_TRANSFER_MANAGER = S3TransferManager.builder().s3Client(S3_ASYNC_CLIENT).build();
    public static final DynamoDbClient DYNAMO_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.DYNAMODB, DynamoDbClient.builder());
    public static final SqsClient SQS_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.SQS, SqsClient.builder());
    public static final StsClient STS_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.STS, StsClient.builder());
    public static final CloudWatchClient CLOUDWATCH_CLIENT_V2 = buildAwsV2Client(CONTAINER, Service.CLOUDWATCH, CloudWatchClient.builder());
    public static final Configuration HADOOP_CONF = getHadoopConfiguration(CONTAINER);

}
