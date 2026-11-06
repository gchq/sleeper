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
package sleeper.statestore.lambda.committer;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
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
import sleeper.local.aws.test.LocalAwsTestBase;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class CompactionCommitBatcherLambdaIT extends LocalAwsTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final CompactionCommitMessageSerDe compactionSerDe = new CompactionCommitMessageSerDe();
    private final StateStoreCommitRequestSerDe stateStoreSerDe = StateStoreCommitRequestSerDe.forFileTransactions();

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
    }

    @Test
    void shouldSendOneCommit() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent("test-table", filesRequest), null);

        // Then
        assertThat(receiveCommitRequests()).containsExactly(
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

    @Test
    void shouldFailWhenInvalidAndValidMessagesAreMixedInOneBatch() {
        // Given
        ReplaceFileReferencesRequest filesRequest = createFilesRequest();
        List<SQSMessage> messages = List.of(
                createMessageWithId("1-valid", new CompactionCommitMessage("table-1", filesRequest)),
                createMessageWithId("2-invalid", "not-a-commit"),
                createMessageWithId("3-valid", new CompactionCommitMessage("table-2", filesRequest)),
                createMessageWithId("4-invalid", "not-a-commit"));

        // When
        SQSBatchResponse response = lambda().handleRequest(createEvent(messages), null);

        // Then
        assertThat(receiveCommitRequests()).containsExactly(
                StateStoreCommitRequest.create("table-1",
                        new ReplaceFileReferencesTransaction(List.of(filesRequest))),
                StateStoreCommitRequest.create("table-2",
                        new ReplaceFileReferencesTransaction(List.of(filesRequest))));
        assertThat(response.getBatchItemFailures())
                .extracting(BatchItemFailure::getItemIdentifier)
                .containsExactly("2-invalid", "4-invalid");
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
        return createEvent(List.of(messages));
    }

    private SQSEvent createEvent(List<SQSMessage> messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(messages);
        return event;
    }

    private SQSMessage createMessage(String tableId, ReplaceFileReferencesRequest request) {
        return createMessageWithId(UUID.randomUUID().toString(), new CompactionCommitMessage(tableId, request));
    }

    private SQSMessage createMessageWithId(String id, CompactionCommitMessage commit) {
        return createMessageWithId(id, compactionSerDe.toJson(commit));
    }

    private SQSMessage createMessageWithId(String id, String body) {
        SQSMessage message = new SQSMessage();
        message.setMessageId(id);
        message.setBody(body);
        return message;
    }

    private List<StateStoreCommitRequest> receiveCommitRequests() {
        return receiveMessages(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .map(stateStoreSerDe::fromJson)
                .toList();
    }
}
