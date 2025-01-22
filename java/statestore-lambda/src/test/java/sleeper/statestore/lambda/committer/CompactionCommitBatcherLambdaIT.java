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
package sleeper.statestore.lambda.committer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.compaction.core.job.commit.CompactionCommitRequestSerDe;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;
import sleeper.localstack.test.SleeperLocalStackContainer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class CompactionCommitBatcherLambdaIT {

    @Container
    private static LocalStackContainer localStackContainer = SleeperLocalStackContainer.create(Service.S3, Service.SQS, Service.DYNAMODB);
    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonSQS sqsClient = buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard());
    private final AmazonDynamoDB dynamoClient = buildAwsV1Client(localStackContainer, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final CompactionCommitRequestSerDe serDe = new CompactionCommitRequestSerDe();
    private final StateStoreCommitRequestSerDe commitSerDe = StateStoreCommitRequestSerDe.forFileTransactions();

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, sqsClient.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")))
                .getQueueUrl());
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
    }

    @Test
    void shouldSendOneCommit() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent(tableProperties.get(TABLE_ID), filesRequest), null);

        // Then
        assertThat(consumeQueueMessages()).containsExactly(
                StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                        new ReplaceFileReferencesTransaction(List.of(filesRequest))));
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    void shouldPassThroughIncorrectTableId() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();
        SQSMessage sqsMessage = createMessage("IncorrectId", filesRequest);

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent(sqsMessage), null);

        // Then
        assertThat(consumeQueueMessages()).containsExactly(
                StateStoreCommitRequest.create("IncorrectId",
                        new ReplaceFileReferencesTransaction(List.of(filesRequest))));
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    void shouldFailWhenUnableToGetToTheSQSQueue() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();
        SQSMessage sqsMessage = createMessage(tableProperties.get(TABLE_ID), filesRequest);

        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, "BROKEN-URL");

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent(sqsMessage), null);

        // Then
        assertThat(response.getBatchItemFailures())
                .extracting(BatchItemFailure::getItemIdentifier)
                .containsExactly(sqsMessage.getMessageId());

    }

    private ReplaceFileReferencesRequest createFilesRequest() {
        PartitionTree partitions = new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree();
        return ReplaceFileReferencesRequest.builder()
                .jobId("test-job")
                .taskId("test-task")
                .jobRunId("test-run")
                .inputFiles(List.of("test.parquet"))
                .newReference(FileReferenceFactory.from(partitions).rootFile("output.parquet", 200))
                .build();
    }

    private CompactionCommitBatcherLambda lambda() {
        return new CompactionCommitBatcherLambda(
                CompactionCommitBatcherLambda.createBatcher(instanceProperties, sqsClient, s3Client));
    }

    private SQSEvent createEvent(String tableId, ReplaceFileReferencesRequest request) {
        return createEvent(createMessage(tableId, request));
    }

    private SQSEvent createEvent(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage createMessage(String tableId, ReplaceFileReferencesRequest request) {
        SQSMessage message = new SQSMessage();
        message.setMessageId(UUID.randomUUID().toString());
        message.setBody(serDe.toJson(tableId, request));
        return message;
    }

    private List<StateStoreCommitRequest> consumeQueueMessages() {
        return sqsClient.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withWaitTimeSeconds(1)
                .withMaxNumberOfMessages(10))
                .getMessages().stream()
                .map(message -> commitSerDe.fromJson(message.getBody()))
                .toList();
    }
}
