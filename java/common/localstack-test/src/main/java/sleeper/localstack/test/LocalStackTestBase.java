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
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.localstack.test.SleeperLocalStackClients.S3_CLIENT;
import static sleeper.localstack.test.SleeperLocalStackClients.SQS_CLIENT;

/**
 * A base class for tests to run against LocalStack.
 */
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class LocalStackTestBase {

    protected final LocalStackContainer localStackContainer = SleeperLocalStackContainer.INSTANCE;
    protected final S3Client s3Client = SleeperLocalStackClients.S3_CLIENT;
    protected final S3AsyncClient s3AsyncClient = SleeperLocalStackClients.S3_ASYNC_CLIENT;
    protected final S3TransferManager s3TransferManager = SleeperLocalStackClients.S3_TRANSFER_MANAGER;
    protected final DynamoDbClient dynamoClient = SleeperLocalStackClients.DYNAMO_CLIENT;
    protected final SqsClient sqsClient = SleeperLocalStackClients.SQS_CLIENT;
    protected final StsClient stsClient = SleeperLocalStackClients.STS_CLIENT;
    protected final Configuration hadoopConf = SleeperLocalStackClients.HADOOP_CONF;
    protected final CloudWatchClient cloudWatchClient = SleeperLocalStackClients.CLOUDWATCH_CLIENT;

    public static void createBucket(String bucketName) {
        S3_CLIENT.createBucket(builder -> builder.bucket(bucketName));
    }

    public static PutObjectResponse putObject(String bucketName, String key, String content) {
        return S3_CLIENT.putObject(builder -> builder.bucket(bucketName).key(key),
                RequestBody.fromString(content));
    }

    public static String getObjectAsString(String bucketName, String key) {
        return S3_CLIENT.getObject(
                builder -> builder.bucket(bucketName).key(key),
                ResponseTransformer.toBytes())
                .asUtf8String();
    }

    public static Set<String> listObjectKeys(String bucketName) {
        return S3_CLIENT.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .collect(toUnmodifiableSet());
    }

    public static String createFifoQueueGetUrl() {
        return SQS_CLIENT.createQueue(CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString() + ".fifo")
                .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true")).build()).queueUrl();
    }

    public static String createSqsQueueGetUrl() {
        return SQS_CLIENT.createQueue(CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString())
                .build())
                .queueUrl();
    }

    public static Stream<String> receiveMessages(String queueUrl) {
        return SQS_CLIENT.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(0)
                .build())
                .messages().stream().map(Message::body);
    }

    public static Stream<Message> receiveMessagesAndMessageGroupId(String queueUrl) {
        return SQS_CLIENT.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(0)
                .messageSystemAttributeNames(List.of(MessageSystemAttributeName.MESSAGE_GROUP_ID))
                .build())
                .messages().stream();
    }

}
