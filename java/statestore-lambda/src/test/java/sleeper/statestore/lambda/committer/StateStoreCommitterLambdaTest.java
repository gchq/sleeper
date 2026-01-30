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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.FilesReportTestHelper;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitter;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class StateStoreCommitterLambdaTest {
    private static final Instant DEFAULT_FILE_UPDATE_TIME = FilesReportTestHelper.DEFAULT_UPDATE_TIME;
    private final Schema schema = createSchemaWithKey("key", new StringType());
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final List<Duration> retryWaits = transactionLogs.getRetryWaits();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_FILE_UPDATE_TIME);

    @BeforeEach
    void setUp() {
        update(stateStore()).initialise(partitions.getAllPartitions());
    }

    @Test
    void shouldApplyOneCommit() throws Exception {
        // Given
        FileReference file = fileFactory.rootFile("test.parquet", 100);

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("test-message", file)),
                null);

        // Then
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(stateStore().getFileReferences())
                .containsExactly(file);
        assertThat(retryWaits).isEmpty();
    }

    @Test
    void shouldFailSomeCommitsInBatch() throws Exception {
        // Given
        FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
        FileReference file2 = fileFactory.rootFile("file-2.parquet", 300);

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("message-1", file1),
                assignJobIdMessage("message-2", assignJobOnPartitionToFiles("job-1", "root", List.of("file-1.parquet"))),
                assignJobIdMessage("message-3", assignJobOnPartitionToFiles("job-2", "root", List.of("file-1.parquet"))),
                addFilesMessage("message-4", file2),
                assignJobIdMessage("message-5", assignJobOnPartitionToFiles("job-3", "root", List.of("file-1.parquet")))),
                null);

        // Then
        assertThat(response.getBatchItemFailures())
                .extracting(BatchItemFailure::getItemIdentifier)
                .containsExactly("message-3", "message-5");
        assertThat(stateStore().getFileReferences())
                .containsExactly(withJobId("job-1", file1), file2);
        assertThat(retryWaits).isEmpty();
    }

    private StateStore stateStore() {
        StateStore stateStore = StateStoreFactory.forCommitterProcess(true, tableProperties,
                transactionLogs.stateStoreBuilder(tableProperties))
                .build();
        stateStore.fixFileUpdateTime(DEFAULT_FILE_UPDATE_TIME);
        return stateStore;
    }

    private StateStoreCommitterLambda lambda() {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        StateStoreProvider stateStoreProvider = FixedStateStoreProvider.singleTable(tableProperties, stateStore());
        return new StateStoreCommitterLambda(
                tablePropertiesProvider, stateStoreProvider,
                serDe(), committer(tablePropertiesProvider, stateStoreProvider), PollWithRetries.noRetries());
    }

    private StateStoreCommitRequestSerDe serDe() {
        return new StateStoreCommitRequestSerDe(new FixedTablePropertiesProvider(tableProperties));
    }

    private StateStoreCommitter committer(TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
        return new StateStoreCommitter(tablePropertiesProvider, stateStoreProvider, transactionLogs.getTransactionBodyStore());
    }

    private SQSEvent event(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage addFilesMessage(String messageId, FileReference... files) {
        StateStoreCommitRequest request = StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                AddFilesTransaction.fromReferences(List.of(files)));
        return sqsMessage(messageId, request);
    }

    private SQSMessage assignJobIdMessage(String messageId, AssignJobIdRequest request) {
        StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                new AssignJobIdsTransaction(List.of(request)));
        return sqsMessage(messageId, commitRequest);
    }

    private SQSMessage sqsMessage(String messageId, StateStoreCommitRequest request) {
        SQSMessage message = new SQSMessage();
        message.setMessageId(messageId);
        message.setBody(new StateStoreCommitRequestSerDe(tableProperties).toJson(request));
        return message;
    }

}
