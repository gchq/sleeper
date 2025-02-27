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
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;

public class TransactionLogFollowerLambdaTest {
    // Tests to create
    // - First entry fails, all fail
    // - Second entry fails, subsequent fails

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = schemaWithKey("key");
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    String tableId = tableProperties.get(TABLE_ID);
    PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
    InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs.forTable(tableProperties));
    InMemoryCompactionJobTracker compactionJobTracker = new InMemoryCompactionJobTracker();
    InMemoryIngestJobTracker ingestJobTracker = new InMemoryIngestJobTracker();

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
        AddFilesTransaction.builder()
                .jobRunIds(jobRunIds)
                .writtenTime(Instant.parse("2025-02-27T13:32:00Z"))
                .fileReferences(List.of(file))
                .build().synchronousCommit(stateStore);

        // When
        streamAllEntriesFromFileTransactionLogToLambda();

        // Then
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
        streamAllEntriesFromFileTransactionLogToLambda();

        // Then
        assertThat(ingestJobTracker.streamTableRecords(tableId)).isEmpty();
    }

    private void streamAllEntriesFromFileTransactionLogToLambda() {
        createLambda().handleRecords(
                transactionLogs.forTable(tableProperties).getFilesLogStore()
                        .readTransactions(TransactionLogRange.fromMinimum(1))
                        .map(entry -> new TransactionLogEntryHandle(tableId, null, entry)));
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
