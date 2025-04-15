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
package sleeper.statestore.lambda.committer;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.compaction.core.job.commit.CompactionCommitMessageSerDe;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class CompactionCommitBatcherLambdaIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final CompactionCommitMessageSerDe compactionSerDe = new CompactionCommitMessageSerDe();
    private final StateStoreCommitRequestSerDe stateStoreSerDe = StateStoreCommitRequestSerDe.forFileTransactions();

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, sqsClient.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")))
                .getQueueUrl());
    }

    @Test
    void shouldSendOneCommit() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent("test-table", filesRequest), null);

        // Then
        assertThat(consumeQueueMessages()).containsExactly(
                StateStoreCommitRequest.create("test-table",
                        new ReplaceFileReferencesTransaction(List.of(filesRequest))));
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    void shouldFailWhenUnableToGetToTheSQSQueue() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();
        SQSMessage sqsMessage = createMessage("test-table", filesRequest);

        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, "BROKEN-URL");

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent(sqsMessage), null);

        // Then
        assertThat(response.getBatchItemFailures())
                .extracting(BatchItemFailure::getItemIdentifier)
                .containsExactly(sqsMessage.getMessageId());

    }

    private ReplaceFileReferencesRequest createFilesRequest() {
        PartitionTree partitions = new PartitionsBuilder(createSchemaWithKey("key")).singlePartition("root").buildTree();
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
        message.setBody(compactionSerDe.toJson(new CompactionCommitMessage(tableId, request)));
        return message;
    }

    private List<StateStoreCommitRequest> consumeQueueMessages() {
        return sqsClient.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withWaitTimeSeconds(1)
                .withMaxNumberOfMessages(10))
                .getMessages().stream()
                .map(message -> stateStoreSerDe.fromJson(message.getBody()))
                .toList();
    }
}
