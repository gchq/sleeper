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
package sleeper.statestore.committer.lambda;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.FilesReportTestHelper;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitRequestDeserialiser;
import sleeper.statestore.committer.StateStoreCommitter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.transactionlog.InMemoryTransactionLogStateStoreTestHelper.inMemoryTransactionLogStateStoreBuilder;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class StateStoreCommitterLambdaTest {
    private static final Instant DEFAULT_FILE_UPDATE_TIME = FilesReportTestHelper.DEFAULT_UPDATE_TIME;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final List<Duration> retryWaits = new ArrayList<>();
    private final InMemoryTransactionLogStore filesLog = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogStore partitionsLog = new InMemoryTransactionLogStore();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_FILE_UPDATE_TIME);

    @BeforeEach
    void setUp() throws StateStoreException {
        stateStore().initialise(partitions.getAllPartitions());
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
        FileReference duplicate1 = fileFactory.rootFile("file-1.parquet", 200);
        FileReference file2 = fileFactory.rootFile("file-2.parquet", 300);
        FileReference duplicate2 = fileFactory.rootFile("file-2.parquet", 400);

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("message-1", file1),
                addFilesMessage("message-2", duplicate1),
                addFilesMessage("message-3", file2),
                addFilesMessage("message-4", duplicate2)),
                null);

        // Then
        assertThat(response.getBatchItemFailures())
                .extracting(BatchItemFailure::getItemIdentifier)
                .containsExactly("message-2", "message-4");
        assertThat(stateStore().getFileReferences())
                .containsExactly(file1, file2);
        assertThat(retryWaits).isEmpty();
    }

    @Test
    void shouldFailFirstAddTransactionWhenItConflictsAndLambdaIsSetToOnlyUpdateOnFailedCommit() throws Exception {
        // Given
        tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT, "false");
        tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH, "false");
        FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
        FileReference file2 = fileFactory.rootFile("file-2.parquet", 200);
        FileReference file3 = fileFactory.rootFile("file-3.parquet", 300);
        stateStore().addFile(file1);
        AtomicInteger addTransactionCalls = new AtomicInteger();
        AtomicInteger readTransactionCalls = new AtomicInteger();
        filesLog.atStartOfAddTransaction(() -> addTransactionCalls.incrementAndGet());
        filesLog.atStartOfReadTransactions(() -> readTransactionCalls.incrementAndGet());

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("file-2-message", file2),
                addFilesMessage("file-3-message", file3)),
                null);

        // Then
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(addTransactionCalls.get()).isEqualTo(3);
        assertThat(readTransactionCalls.get()).isEqualTo(1);
        assertThat(stateStore().getFileReferences())
                .containsExactly(file1, file2, file3);
        assertThat(retryWaits).isEmpty();
    }

    @Test
    void shouldSucceedFirstAddTransactionWhenItConflictsAndLambdaIsSetToUpdateOnEveryCommit() throws Exception {
        // Given
        tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT, "true");
        tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH, "false");
        FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
        FileReference file2 = fileFactory.rootFile("file-2.parquet", 200);
        FileReference file3 = fileFactory.rootFile("file-3.parquet", 300);
        stateStore().addFile(file1);
        AtomicInteger addTransactionCalls = new AtomicInteger();
        AtomicInteger readTransactionCalls = new AtomicInteger();
        filesLog.atStartOfAddTransaction(() -> addTransactionCalls.incrementAndGet());
        filesLog.atStartOfReadTransactions(() -> readTransactionCalls.incrementAndGet());

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("file-2-message", file2),
                addFilesMessage("file-3-message", file3)),
                null);

        // Then
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(addTransactionCalls.get()).isEqualTo(2);
        assertThat(readTransactionCalls.get()).isEqualTo(2);
        assertThat(stateStore().getFileReferences())
                .containsExactly(file1, file2, file3);
        assertThat(retryWaits).isEmpty();
    }

    @Test
    @Disabled("TODO")
    void shouldSucceedFirstAddTransactionWhenItConflictsAndLambdaIsSetToUpdateOnEveryBatch() throws Exception {
        // Given
        tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT, "false");
        tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH, "true");
        FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
        FileReference file2 = fileFactory.rootFile("file-2.parquet", 200);
        FileReference file3 = fileFactory.rootFile("file-3.parquet", 300);
        stateStore().addFile(file1);
        AtomicInteger addTransactionCalls = new AtomicInteger();
        AtomicInteger readTransactionCalls = new AtomicInteger();
        filesLog.atStartOfAddTransaction(() -> addTransactionCalls.incrementAndGet());
        filesLog.atStartOfReadTransactions(() -> readTransactionCalls.incrementAndGet());

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                addFilesMessage("file-2-message", file2),
                addFilesMessage("file-3-message", file3)),
                null);

        // Then
        assertThat(response.getBatchItemFailures()).isEmpty();
        assertThat(addTransactionCalls.get()).isEqualTo(2);
        assertThat(readTransactionCalls.get()).isEqualTo(1);
        assertThat(stateStore().getFileReferences())
                .containsExactly(file1, file2, file3);
        assertThat(retryWaits).isEmpty();
    }

    private StateStore stateStore() {
        StateStore stateStore = StateStoreFactory.forCommitterProcess(true, tableProperties,
                inMemoryTransactionLogStateStoreBuilder(tableProperties.getStatus(), schema, recordWaits(retryWaits)))
                .filesLogStore(filesLog)
                .partitionsLogStore(partitionsLog)
                .build();
        stateStore.fixFileUpdateTime(DEFAULT_FILE_UPDATE_TIME);
        return stateStore;
    }

    private StateStoreCommitterLambda lambda() {
        return new StateStoreCommitterLambda(
                new FixedTablePropertiesProvider(tableProperties),
                deserialiser(), committer(), PollWithRetries.noRetries());
    }

    private StateStoreCommitRequestDeserialiser deserialiser() {
        return new StateStoreCommitRequestDeserialiser(
                new FixedTablePropertiesProvider(tableProperties),
                s3Key -> {
                    throw new IllegalArgumentException("Unexpected request to load from data bucket key " + s3Key);
                });
    }

    private StateStoreCommitter committer() {
        return new StateStoreCommitter(CompactionJobStatusStore.NONE, IngestJobStatusStore.NONE,
                Map.of(tableProperties.get(TABLE_ID), stateStore())::get,
                Instant::now);
    }

    private SQSEvent event(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage addFilesMessage(String messageId, FileReference... files) {
        IngestAddFilesCommitRequest request = IngestAddFilesCommitRequest.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .fileReferences(List.of(files))
                .build();
        SQSMessage message = new SQSMessage();
        message.setMessageId(messageId);
        message.setBody(new IngestAddFilesCommitRequestSerDe().toJson(request));
        return message;
    }

}
