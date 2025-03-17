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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
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
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionBodyStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionFinishedEvent;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionStartedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
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
    Supplier<String> itemIdentifierSupplier = supplyNumberedIdsWithPrefix("item-");

    @Test
    void shouldUpdateIngestJobTrackerWhenFileWasAdded() {
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
        assertThat(response).isEqualTo(new StreamsEventResponse());
        assertThat(ingestJobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(Instant.parse("2025-02-27T13:31:00Z"), 2),
                                ingestAddedFilesStatus(Instant.parse("2025-02-27T13:32:00Z"), 1))));
    }

    @Test
    void shouldUpdateCompactionJobTrackerWhenCompactionWasCommitted() {
        // Given
        FileReference input = fileFactory().rootFile("input.parquet", 100);
        FileReference output = fileFactory().rootFile("output.parquet", 50);
        var createTime = Instant.parse("2025-03-04T08:57:00Z");
        var startTime = Instant.parse("2025-03-04T08:58:00Z");
        var finishTime = Instant.parse("2025-03-04T08:59:00Z");
        var commitTime = Instant.parse("2025-03-04T08:59:05Z");
        var job = CompactionJobCreatedEvent.builder()
                .jobId("test-job")
                .tableId(tableId)
                .partitionId("root")
                .inputFilesCount(1)
                .build();
        var run = compactionStartedEventBuilder(job, startTime).taskId("test-task").jobRunId("test-run").build();
        var finished = compactionFinishedEvent(run, summary(startTime, finishTime, 100, 50));
        compactionJobTracker.jobCreated(job, createTime);
        compactionJobTracker.jobStarted(run);
        compactionJobTracker.jobFinished(finished);
        update(stateStore).addFile(input);
        update(stateStore).assignJobId("test-job", "root", List.of("input.parquet"));
        stateStore.fixFileUpdateTime(commitTime);
        update(stateStore).addTransaction(new ReplaceFileReferencesTransaction(List.of(
                ReplaceFileReferencesRequest.builder()
                        .jobId("test-job")
                        .taskId("test-task")
                        .jobRunId("test-run")
                        .inputFiles(List.of("input.parquet"))
                        .newReference(output)
                        .build())));

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then
        assertThat(response).isEqualTo(new StreamsEventResponse());
        assertThat(compactionJobTracker.getAllJobs(tableId)).containsExactly(
                compactionJobCreated(job, createTime, jobRunOnTask("test-task",
                        compactionStartedStatus(startTime),
                        compactionFinishedStatus(summary(startTime, finishTime, 100, 50)),
                        compactionCommittedStatus(commitTime))));
    }

    @Test
    void shouldNotUpdateCompactionJobTrackerWhenDisabled() {
        // Given
        instanceProperties.set(COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED, "false");
        FileReference input = fileFactory().rootFile("input.parquet", 100);
        FileReference output = fileFactory().rootFile("output.parquet", 50);
        var createTime = Instant.parse("2025-03-04T08:57:00Z");
        var startTime = Instant.parse("2025-03-04T08:58:00Z");
        var finishTime = Instant.parse("2025-03-04T08:59:00Z");
        var commitTime = Instant.parse("2025-03-04T08:59:05Z");
        var job = CompactionJobCreatedEvent.builder()
                .jobId("test-job")
                .tableId(tableId)
                .partitionId("root")
                .inputFilesCount(1)
                .build();
        var run = compactionStartedEventBuilder(job, startTime).taskId("test-task").jobRunId("test-run").build();
        var finished = compactionFinishedEvent(run, summary(startTime, finishTime, 100, 50));
        compactionJobTracker.jobCreated(job, createTime);
        compactionJobTracker.jobStarted(run);
        compactionJobTracker.jobFinished(finished);
        update(stateStore).addFile(input);
        update(stateStore).assignJobId("test-job", "root", List.of("input.parquet"));
        stateStore.fixFileUpdateTime(commitTime);
        update(stateStore).addTransaction(new ReplaceFileReferencesTransaction(List.of(
                ReplaceFileReferencesRequest.builder()
                        .jobId("test-job")
                        .taskId("test-task")
                        .jobRunId("test-run")
                        .inputFiles(List.of("input.parquet"))
                        .newReference(output)
                        .build())));

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then
        assertThat(response).isEqualTo(new StreamsEventResponse());
        assertThat(compactionJobTracker.getAllJobs(tableId)).containsExactly(
                compactionJobCreated(job, createTime, jobRunOnTask("test-task",
                        compactionStartedStatus(startTime),
                        compactionFinishedStatus(summary(startTime, finishTime, 100, 50)))));
    }

    @Test
    void shouldProcessEntrySuccessfullyWithNoTrackerUpdate() {
        // Given
        FileReference file = fileFactory().rootFile("test.parquet", 100);
        AddFilesTransaction.fromReferences(List.of(file)).synchronousCommit(stateStore);

        // When
        StreamsEventResponse response = streamAllEntriesFromFileTransactionLogToLambda();

        // Then
        assertThat(response).isEqualTo(new StreamsEventResponse());
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
        assertThat(response).isEqualTo(new StreamsEventResponse());
        assertThat(ingestJobTracker.streamTableRecords(tableId)).isEmpty();
        assertThat(ingestJobTracker.streamTableRecords("table-gone")).isEmpty();
    }

    @Test
    void shouldIgnoreUnexpectedExceptionAndProcessFurtherEntries() {
        // Given two transactions to add files
        FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
        IngestJobRunIds job1RunIds = IngestJobRunIds.builder()
                .tableId(tableId)
                .jobId("job-1")
                .jobRunId("run-1")
                .taskId("test-task")
                .build();
        ingestJobTracker.jobStarted(IngestJobStartedEvent.builder()
                .jobRunIds(job1RunIds)
                .fileCount(1)
                .startTime(Instant.parse("2025-02-27T13:31:00Z"))
                .build());
        update(stateStore).addTransaction(AddFilesTransaction.builder()
                .jobRunIds(job1RunIds)
                .writtenTime(Instant.parse("2025-02-27T13:32:00Z"))
                .fileReferences(List.of(file1))
                .build());
        FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
        IngestJobRunIds job2RunIds = IngestJobRunIds.builder()
                .tableId(tableId)
                .jobId("job-2")
                .jobRunId("run-2")
                .taskId("test-task")
                .build();
        ingestJobTracker.jobStarted(IngestJobStartedEvent.builder()
                .jobRunIds(job2RunIds)
                .fileCount(2)
                .startTime(Instant.parse("2025-02-27T14:31:00Z"))
                .build());
        update(stateStore).addTransaction(AddFilesTransaction.builder()
                .jobRunIds(job2RunIds)
                .writtenTime(Instant.parse("2025-02-27T14:32:00Z"))
                .fileReferences(List.of(file2))
                .build());

        // When we replace the first entry with an entry that cannot be read
        transactionBodyStore.store("transaction/fake", tableId, new ClearFilesTransaction());
        StreamsEventResponse response = createLambda().handleRecords(Stream.of(
                fakeEntryHandle(new TransactionLogEntry(
                        1, Instant.parse("2025-02-27T13:32:00Z"), TransactionType.ADD_FILES, "transaction/fake")),
                handleForTransaction(2)));

        // Then the second transaction is applied to the job tracker
        assertThat(response).isEqualTo(new StreamsEventResponse());
        assertThat(ingestJobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("job-2",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(Instant.parse("2025-02-27T14:31:00Z"), 2),
                                ingestAddedFilesStatus(Instant.parse("2025-02-27T14:32:00Z"), 1))),
                ingestJobStatus("job-1",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(Instant.parse("2025-02-27T13:31:00Z"), 1))));
    }

    private StreamsEventResponse streamAllEntriesFromFileTransactionLogToLambda() {
        return createLambda().handleRecords(
                transactionLogs.forTable(tableProperties).getFilesLogStore()
                        .readTransactions(TransactionLogRange.fromMinimum(1))
                        .map(entry -> new TransactionLogEntryHandle(tableId, itemIdentifierSupplier.get(), entry)));
    }

    private TransactionLogEntryHandle fakeEntryHandle(TransactionLogEntry entry) {
        return new TransactionLogEntryHandle(tableId, itemIdentifierSupplier.get(), entry);
    }

    private TransactionLogEntryHandle handleForTransaction(long transactionNumber) {
        return handleForTransaction(transactionNumber, itemIdentifierSupplier.get());
    }

    private TransactionLogEntryHandle handleForTransaction(long transactionNumber, String itemIdentifier) {
        return transactionLogs.forTable(tableProperties).getFilesLogStore()
                .readTransactions(new TransactionLogRange(transactionNumber, transactionNumber + 1))
                .map(entry -> new TransactionLogEntryHandle(tableId, itemIdentifier, entry))
                .findFirst().orElseThrow();
    }

    private TransactionLogFollowerLambda createLambda() {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        DynamoDBStreamTransactionLogEntryMapper mapper = new DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider.from(tablePropertiesProvider));
        StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
        return new TransactionLogFollowerLambda(instanceProperties, tablePropertiesProvider, mapper, stateStoreProvider, compactionJobTracker, ingestJobTracker);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(stateStore, Instant.parse("2025-02-27T13:33:00Z"));
    }
}
