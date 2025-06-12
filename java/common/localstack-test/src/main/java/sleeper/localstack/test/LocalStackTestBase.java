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

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.s3.AmazonS3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.localstack.test.SleeperLocalStackClients.S3_CLIENT_V2;
import static sleeper.localstack.test.SleeperLocalStackClients.SQS_CLIENT_V2;

/**
 * A base class for tests to run against LocalStack.
 */
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class LocalStackTestBase {

    protected final LocalStackContainer localStackContainer = SleeperLocalStackContainer.INSTANCE;
    protected final AmazonS3 s3Client = SleeperLocalStackClients.S3_CLIENT;
    protected final AmazonCloudWatch cloudWatchClient = SleeperLocalStackClients.CLOUDWATCH_CLIENT;
    protected final S3Client s3ClientV2 = SleeperLocalStackClients.S3_CLIENT_V2;
    protected final S3AsyncClient s3AsyncClient = SleeperLocalStackClients.S3_ASYNC_CLIENT;
    protected final S3TransferManager s3TransferManager = SleeperLocalStackClients.S3_TRANSFER_MANAGER;
    protected final DynamoDbClient dynamoClientV2 = SleeperLocalStackClients.DYNAMO_CLIENT_V2;
    protected final SqsClient sqsClientV2 = SleeperLocalStackClients.SQS_CLIENT_V2;
    protected final StsClient stsClientV2 = SleeperLocalStackClients.STS_CLIENT_V2;
    protected final Configuration hadoopConf = SleeperLocalStackClients.HADOOP_CONF;
    protected final CloudWatchClient cloudWatchClientV2 = SleeperLocalStackClients.CLOUDWATCH_CLIENT_V2;

    public static void createBucket(String bucketName) {
        S3_CLIENT_V2.createBucket(builder -> builder.bucket(bucketName));
    }

    public static PutObjectResponse putObject(String bucketName, String key, String content) {
        return S3_CLIENT_V2.putObject(builder -> builder.bucket(bucketName).key(key),
                RequestBody.fromString(content));
    }

    public static String getObjectAsString(String bucketName, String key) {
        return S3_CLIENT_V2.getObject(
                builder -> builder.bucket(bucketName).key(key),
                ResponseTransformer.toBytes())
                .asUtf8String();
    }

    public static Set<String> listObjectKeys(String bucketName) {
        return S3_CLIENT_V2.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .collect(toUnmodifiableSet());
    }

    public static String createFifoQueueGetUrl() {
        return SQS_CLIENT_V2.createQueue(CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString() + ".fifo")
                .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true")).build()).queueUrl();
    }

    public static String createSqsQueueGetUrl() {
        return SQS_CLIENT_V2.createQueue(CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString())
                .build())
                .queueUrl();
    }

}
