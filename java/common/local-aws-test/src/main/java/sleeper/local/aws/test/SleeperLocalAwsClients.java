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
package sleeper.local.aws.test;

import io.floci.testcontainers.FlociContainer;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import static sleeper.local.aws.test.LocalAwsHadoopConfigurationProvider.getHadoopConfiguration;
import static sleeper.local.aws.test.LocalAwsV2ClientHelper.buildAwsCredentialsProvider;
import static sleeper.local.aws.test.LocalAwsV2ClientHelper.buildAwsV2Client;

/**
 * Constants for shared AWS clients pointing to a local AWS test container.
 */
public class SleeperLocalAwsClients {

    private SleeperLocalAwsClients() {
    }

    private static final FlociContainer CONTAINER = SleeperLocalAwsContainer.INSTANCE;
    public static final S3Client S3_CLIENT = buildAwsV2Client(CONTAINER, S3Client.builder());
    public static final S3AsyncClient S3_ASYNC_CLIENT = buildAwsV2Client(CONTAINER, S3AsyncClient.crtBuilder());
    public static final S3TransferManager S3_TRANSFER_MANAGER = S3TransferManager.builder().s3Client(S3_ASYNC_CLIENT).build();
    public static final DynamoDbClient DYNAMO_CLIENT = buildAwsV2Client(CONTAINER, DynamoDbClient.builder());
    public static final SqsClient SQS_CLIENT = buildAwsV2Client(CONTAINER, SqsClient.builder());
    public static final StsClient STS_CLIENT = buildAwsV2Client(CONTAINER, StsClient.builder());
    public static final CloudWatchClient CLOUDWATCH_CLIENT = buildAwsV2Client(CONTAINER, CloudWatchClient.builder());
    public static final Configuration HADOOP_CONF = getHadoopConfiguration(CONTAINER);
    public static final AwsCredentialsProvider CREDENTIALS_PROVIDER = buildAwsCredentialsProvider();

}
