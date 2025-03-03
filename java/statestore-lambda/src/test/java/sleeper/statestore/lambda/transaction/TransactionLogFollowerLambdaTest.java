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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse.BatchItemFailure;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionBodyStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;

public class TransactionLogFollowerLambdaTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = schemaWithKey("key");
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    String tableId = tableProperties.get(TABLE_ID);
    PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
    InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    InMemoryTransactionBodyStore transactionBodyStore = transactionLogs.getTransactionBodyStore();
    StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs.forTable(tableProperties));
    InMemoryCompactionJobTracker compactionJobTracker = new InMemoryCompactionJobTracker();
    InMemoryIngestJobTracker ingestJobTracker = new InMemoryIngestJobTracker();

    // Tests remaining
    //  - Failure on statestore
    //  - Failure within trackers

    @Test
    void shouldProcessEntrySuccessfullyTriggersTrackerUpdate() {
        // Given
        FileReference file = fileFactory().rootFile("test.parquet", 100);
        IngestJobRunIds jobRunIds = IngestJobRunIds.builder()
                .tableId(tableId)
                .jobId("test-job")
                .jobRunId("test-run")
                .taskId("test-task")
                .build();
        ingestJobTracker.jobStarted(IngestJobStartedEvent.builder()
                .jobRunIds(jobRunIds)
                .fileCount(2)
                .startTime(Instant.parse("2025-02-27T13:31:00Z"))
                .build());

        update(stateStore).addTransaction(AddFilesTransaction.builder()
                .jobRunIds(jobRunIds)
                .writtenTime(Instant.parse("2025-02-27T13:32:00Z"))
                .fileReferences(List.of(file))
                .build());

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then
        assertThat(response).isEqualTo(new StreamsEventResponse(List.of()));
        assertThat(ingestJobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(Instant.parse("2025-02-27T13:31:00Z"), 2),
                                ingestAddedFilesStatus(Instant.parse("2025-02-27T13:32:00Z"), 1))));
    }

    @Test
    void shouldProcessEntrySuccessfullyWithNoTrackerUpdate() {
        // Given
        FileReference file = fileFactory().rootFile("test.parquet", 100);
        AddFilesTransaction.fromReferences(List.of(file)).synchronousCommit(stateStore);

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then
        assertThat(response).isEqualTo(new StreamsEventResponse(List.of()));
        assertThat(ingestJobTracker.streamTableRecords(tableId)).isEmpty();
    }

    @Test
    void shouldIgnoreEntryWhenTableDoesNotExist() {
        // Given
        FileReference file = fileFactory().rootFile("test.parquet", 100);
        TransactionLogEntryHandle entry = new TransactionLogEntryHandle("table-gone", "test-item",
                new TransactionLogEntry(1L, Instant.now(),
                        AddFilesTransaction.builder()
                                .jobId("test-job")
                                .jobRunId("test-run")
                                .taskId("test-task")
                                .writtenTime(Instant.parse("2025-02-27T13:32:00Z"))
                                .fileReferences(List.of(file))
                                .build()));

        // When
        StreamsEventResponse response = createLambda().handleRecords(Stream.of(entry));

        // Then
        assertThat(response).isEqualTo(new StreamsEventResponse(List.of()));
        assertThat(ingestJobTracker.streamTableRecords(tableId)).isEmpty();
        assertThat(ingestJobTracker.streamTableRecords("table-gone")).isEmpty();
    }

    @Test
    void shouldFailOnUnexpectedExceptionAndNotProcessFurtherEntries() {
        // Given one transaction that cannot be read
        FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
        transactionBodyStore.setStoreTransactionsWithObjectKeys(List.of("transaction/1", "transaction/2"));
        update(stateStore).addFile(file1);
        transactionBodyStore.store("transaction/1", tableId, new ClearFilesTransaction());
        // And then one transaction that would be successfully added to the tracker
        FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
        IngestJobRunIds jobRunIds = IngestJobRunIds.builder()
                .tableId(tableId)
                .jobId("test-job")
                .jobRunId("test-run")
                .taskId("test-task")
                .build();
        ingestJobTracker.jobStarted(IngestJobStartedEvent.builder()
                .jobRunIds(jobRunIds)
                .fileCount(2)
                .startTime(Instant.parse("2025-02-27T13:31:00Z"))
                .build());
        update(stateStore).addTransaction(AddFilesTransaction.builder()
                .jobRunIds(jobRunIds)
                .writtenTime(Instant.parse("2025-02-27T13:32:00Z"))
                .fileReferences(List.of(file2))
                .build());

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then neither transaction is applied
        assertThat(response).isEqualTo(new StreamsEventResponse(List.of(
                new BatchItemFailure("item-1"),
                new BatchItemFailure("item-2"))));
        assertThat(ingestJobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(Instant.parse("2025-02-27T13:31:00Z"), 2))));
    }

    @Test
    void shouldFailOnUnexpectedExceptionAfterProcessingEntries() {
        // Given one transaction to be successfully added to the tracker
        FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
        IngestJobRunIds jobRunIds = IngestJobRunIds.builder()
                .tableId(tableId)
                .jobId("test-job")
                .jobRunId("test-run")
                .taskId("test-task")
                .build();
        ingestJobTracker.jobStarted(IngestJobStartedEvent.builder()
                .jobRunIds(jobRunIds)
                .fileCount(2)
                .startTime(Instant.parse("2025-02-27T13:31:00Z"))
                .build());
        transactionBodyStore.setStoreTransactionsWithObjectKeys(List.of("transaction/1", "transaction/2"));
        update(stateStore).addTransaction(AddFilesTransaction.builder()
                .jobRunIds(jobRunIds)
                .writtenTime(Instant.parse("2025-02-27T13:32:00Z"))
                .fileReferences(List.of(file1))
                .build());
        // And then one transaction which cannot be read
        FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
        update(stateStore).addFile(file2);
        transactionBodyStore.store("transaction/2", tableId, new ClearFilesTransaction());

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then the first transaction is applied
        assertThat(response).isEqualTo(new StreamsEventResponse(List.of(
                new BatchItemFailure("item-2"))));
        assertThat(ingestJobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(Instant.parse("2025-02-27T13:31:00Z"), 2),
                                ingestAddedFilesStatus(Instant.parse("2025-02-27T13:32:00Z"), 1))));
    }

    private StreamsEventResponse streamAllEntriesFromFileTransactionLogToLambda() {
        return createLambda().handleRecords(
                transactionLogs.forTable(tableProperties).getFilesLogStore()
                        .readTransactions(TransactionLogRange.fromMinimum(1))
                        .map(entry -> new TransactionLogEntryHandle(tableId, "item-" + entry.getTransactionNumber(), entry)));
    }

    private TransactionLogFollowerLambda createLambda() {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        DynamoDBStreamTransactionLogEntryMapper mapper = new DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider.from(tablePropertiesProvider));
        StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
        return new TransactionLogFollowerLambda(mapper, tablePropertiesProvider, stateStoreProvider, compactionJobTracker, ingestJobTracker);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(stateStore, Instant.parse("2025-02-27T13:33:00Z"));
    }
}
