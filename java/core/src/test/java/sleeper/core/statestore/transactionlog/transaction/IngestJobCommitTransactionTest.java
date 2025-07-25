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
package sleeper.core.statestore.transactionlog.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStoreIngestTrackerTestBase;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatusUncommitted;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.failedStatus;

public class IngestJobCommitTransactionTest extends InMemoryTransactionLogStateStoreIngestTrackerTestBase {

    private TransactionLogStateStore committerStore;
    private TransactionLogStateStore followerStore;

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(createSchemaWithKey("key", new LongType())).singlePartition("root"));
        committerStore = (TransactionLogStateStore) super.store;
        followerStore = stateStoreBuilder(createSchemaWithKey("key", new LongType())).build();
    }

    @Test
    void shouldCommitIngestJob() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        trackJobRun("test-job", "test-run", 1, file);
        AddFilesTransaction transaction = AddFilesTransaction.builder()
                .files(AllReferencesToAFile.newFilesWithReferences(List.of(file)))
                .jobId("test-job")
                .taskId(DEFAULT_TASK_ID)
                .jobRunId("test-run")
                .writtenTime(DEFAULT_COMMIT_TIME)
                .build();

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(file);
        assertThat(tracker.getAllJobs(tableId))
                .containsExactly(ingestJobStatus("test-job", jobRunOnTask(DEFAULT_TASK_ID,
                        ingestStartedStatus(DEFAULT_START_TIME),
                        ingestFinishedStatusUncommitted(defaultSummary(100)),
                        ingestAddedFilesStatus(DEFAULT_COMMIT_TIME, 1))));
    }

    @Test
    void shouldNotUpdateTrackerWhenCommitIsNotForAnyIngestJob() {
        // Given we have a commit request without an ingest job (e.g. from an endless stream of rows)
        FileReference file = factory.rootFile("file.parquet", 100L);
        AddFilesTransaction transaction = AddFilesTransaction.fromReferences(List.of(file));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(file);
        assertThat(tracker.getAllJobs(tableId)).isEmpty();
    }

    @Test
    void shouldFailWhenFileAlreadyExists() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        addTransactionWithTracking(AddFilesTransaction.fromReferences(List.of(file)));
        trackJobRun("test-job", "test-run", 1, file);
        AddFilesTransaction transaction = AddFilesTransaction.builder()
                .files(AllReferencesToAFile.newFilesWithReferences(List.of(file)))
                .jobId("test-job")
                .jobRunId("test-run")
                .taskId(DEFAULT_TASK_ID)
                .writtenTime(DEFAULT_COMMIT_TIME)
                .build();

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(file);
        assertThat(tracker.getAllJobs(tableId))
                .containsExactly(ingestJobStatus("test-job",
                        jobRunOnTask(DEFAULT_TASK_ID,
                                ingestStartedStatus(DEFAULT_START_TIME),
                                ingestFinishedStatusUncommitted(defaultSummary(100)),
                                failedStatus(DEFAULT_COMMIT_TIME, List.of("File already exists: file.parquet")))));
    }

    @Test
    void shouldFailWhenOtherProcessMakesConflictingSynchronousCommitOfSameFile() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        trackJobRun("test-job", "test-run", 1, file);
        trackJobRun("test-job", "other-run", 1, file);
        AddFilesTransaction.Builder transactionBuilder = AddFilesTransaction.builder()
                .files(AllReferencesToAFile.newFilesWithReferences(List.of(file)))
                .jobId("test-job")
                .taskId(DEFAULT_TASK_ID)
                .writtenTime(DEFAULT_COMMIT_TIME);
        AddFilesTransaction testTransaction = transactionBuilder.jobRunId("test-run").build();
        AddFilesTransaction otherTransaction = transactionBuilder.jobRunId("other-run").build();
        filesLogStore.atStartOfNextAddTransaction(() -> {
            otherTransaction.synchronousCommit(store);
        });

        // When / Then
        assertThatThrownBy(() -> testTransaction.synchronousCommit(store))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(followerStore.getFileReferences()).containsExactly(file);
    }

    private void addTransactionWithTracking(AddFilesTransaction transaction) {
        // Transaction is added in a committer process
        committerStore.addTransaction(AddTransactionRequest.withTransaction(transaction).build());

        // Job tracker updates are done in a separate process that reads from the log and updates its local state
        TransactionLogEntry entry = filesLogStore.getLastEntry();
        followerStore.applyEntryFromLog(entry, StateListenerBeforeApply.updateTrackers(sleeperTable, tracker, CompactionJobTracker.NONE));
    }

}
