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
package sleeper.core.statestore.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.AFTER_DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionFinishedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionStartedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRunBuilder;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

public class TransactionLogStateStoreCompactionCommitByTransactionTest extends InMemoryTransactionLogStateStoreTestBase {

    private static final String DEFAULT_TASK_ID = "test-task";
    private static final Instant DEFAULT_CREATE_TIME = Instant.parse("2025-01-14T11:40:00Z");
    private static final Instant DEFAULT_START_TIME = Instant.parse("2025-01-14T11:41:00Z");
    private static final Instant DEFAULT_FINISH_TIME = Instant.parse("2025-01-14T11:42:00Z");
    private static final Instant DEFAULT_COMMIT_TIME = Instant.parse("2025-01-14T11:42:10Z");

    private final Schema schema = schemaWithKey("key", new StringType());
    private final InMemoryCompactionJobTracker tracker = new InMemoryCompactionJobTracker();
    private TransactionLogStateStore store;

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(schema).singlePartition("root"));
        store = (TransactionLogStateStore) super.store;
    }

    @Test
    public void shouldCommitCompaction() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        store.addFiles(List.of(oldFile));
        store.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("oldFile"))));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "test-run");

        // When
        addTransactionWithTracking(new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("test-run").build())));

        // Then
        assertThat(store.getFileReferences()).containsExactly(newFile);
        assertThat(store.getFileReferencesWithNoJobId()).containsExactly(newFile);
        assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                .containsExactly("oldFile");
        assertThat(store.getPartitionToReferencedFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files -> assertThat(files).containsExactly("newFile"));
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId()))
                .containsExactly(defaultStatus(trackedJob, defaultCommittedRun(100)));
    }

    @Test
    void shouldIgnoreCompactionCommitWhenAlreadyCommitted() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        store.addFiles(List.of(oldFile));
        store.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("oldFile"))));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        addTransactionWithTracking(new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("run1").build())));
        trackJobRun(trackedJob, "run2");

        // When
        addTransactionWithTracking(new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("run2").build())));

        // Then
        assertThat(store.getFileReferences()).containsExactly(newFile);
        assertThat(store.getFileReferencesWithNoJobId()).containsExactly(newFile);
        assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                .containsExactly("oldFile");
        assertThat(store.getPartitionToReferencedFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files -> assertThat(files).containsExactly("newFile"));
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob,
                        defaultCommittedRun(100),
                        defaultFailedCommitRun(100, List.of("File reference not found in partition root, filename oldFile"))));
    }

    @Test
    void shouldIgnoreCompactionCommitWhenInputFilesAreNotAssignedToJob() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        store.addFile(oldFile);
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(store.getFileReferences()).containsExactly(oldFile);
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100,
                        List.of("Reference to file is not assigned to job job1, in partition root, filename oldFile"))));
    }

    @Test
    public void shouldIgnoreCompactionCommitWhenInputFileIsNotInStateStore() {
        // Given
        FileReference newFile = factory.rootFile("newFile", 100L);
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("run1").build()));

        // When we commit a compaction with an input file that is not in the state store, e.g. because the
        // compaction has already been committed, and the file has already been garbage collected.
        addTransactionWithTracking(transaction);

        // Then
        assertThat(store.getFileReferences()).isEmpty();
        assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100, List.of("File not found: oldFile"))));
    }

    @Test
    public void shouldIgnoreCompactionCommitWhenOneInputFileIsNotInStateStore() {
        // Given
        FileReference oldFile1 = factory.rootFile("oldFile1", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        store.addFile(oldFile1);
        store.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("oldFile1"))));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 2);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile1", "oldFile2"), newFile).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(store.getFileReferences()).containsExactly(withJobId("job1", oldFile1));
        assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
        assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100, List.of("File not found: oldFile2"))));
    }

    @Test
    public void shouldIgnoreCompactionCommitWhenFileReferenceDoesNotExistInPartition() {
        // Given
        splitPartition("root", "L", "R", "5");
        FileReference file = factory.rootFile("file", 100L);
        FileReference existingReference = splitFile(file, "L");
        store.addFile(existingReference);
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("file"), factory.rootFile("file2", 100L)).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(store.getFileReferences()).containsExactly(existingReference);
        assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100,
                        List.of("File reference not found in partition root, filename file"))));
    }

    @Test
    void shouldFailWhenFileToBeMarkedReadyForGCHasSameFileNameAsNewFile() {
        // Given
        FileReference file = factory.rootFile("file1", 100L);
        store.addFile(file);
        store.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("file1"))));

        // When / Then
        assertThatThrownBy(() -> new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferences("job1", List.of("file1"), file))))
                .isInstanceOf(NewReferenceSameAsOldReferenceException.class);
    }

    @Test
    public void shouldIgnoreCompactionCommitWhenOutputFileAlreadyExists() {
        // Given
        splitPartition("root", "L", "R", "m");
        FileReference file = factory.rootFile("oldFile", 100L);
        FileReference existingReference = splitFile(file, "L");
        FileReference newReference = factory.partitionFile("L", "newFile", 100L);
        store.addFiles(List.of(existingReference, newReference));
        store.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "L", List.of("oldFile"))));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newReference).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                withJobId("job1", existingReference), newReference);
        assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100,
                        List.of("File already exists: newFile"))));
    }

    private ReplaceFileReferencesRequest.Builder replaceJobFileReferencesBuilder(String jobId, List<String> inputFiles, FileReference newReference) {
        return ReplaceFileReferencesRequest.builder()
                .jobId(jobId)
                .inputFiles(inputFiles)
                .newReference(newReference)
                .taskId(DEFAULT_TASK_ID);
    }

    private void addTransactionWithTracking(ReplaceFileReferencesTransaction transaction) {
        store.addTransaction(AddTransactionRequest.withTransaction(transaction)
                .beforeApplyListener((number, state) -> {
                    transaction.reportJobs(tracker, sleeperTable, state, DEFAULT_COMMIT_TIME);
                }).build());
    }

    private CompactionJobCreatedEvent trackJobCreated(String jobId, String partitionId, int inputFilesCount) {
        CompactionJobCreatedEvent job = trackedJob(jobId, partitionId, inputFilesCount);
        tracker.jobCreated(job, DEFAULT_CREATE_TIME);
        return job;
    }

    private void trackJobRun(CompactionJobCreatedEvent job, String jobRunId) {
        tracker.jobStarted(compactionStartedEventBuilder(job, DEFAULT_START_TIME).taskId(DEFAULT_TASK_ID).jobRunId(jobRunId).build());
        tracker.jobFinished(compactionFinishedEventBuilder(job, defaultSummary(100)).taskId(DEFAULT_TASK_ID).jobRunId(jobRunId).build());
    }

    private CompactionJobCreatedEvent trackedJob(String jobId, String partitionId, int inputFilesCount) {
        return CompactionJobCreatedEvent.builder()
                .jobId(jobId)
                .tableId(tableId)
                .partitionId(partitionId)
                .inputFilesCount(inputFilesCount)
                .build();
    }

    private JobRunSummary defaultSummary(long numberOfRecords) {
        return summary(DEFAULT_START_TIME, DEFAULT_FINISH_TIME, numberOfRecords, numberOfRecords);
    }

    private CompactionJobStatus defaultStatus(CompactionJobCreatedEvent job, JobRun... runs) {
        return compactionJobCreated(job, DEFAULT_CREATE_TIME, runs);
    }

    private JobRun defaultCommittedRun(int numberOfRecords) {
        return finishedCompactionRun(DEFAULT_TASK_ID, defaultSummary(numberOfRecords), DEFAULT_COMMIT_TIME);
    }

    private JobRun defaultFailedCommitRun(int numberOfRecords, List<String> reasons) {
        return finishedCompactionRunBuilder(DEFAULT_TASK_ID, defaultSummary(numberOfRecords))
                .statusUpdate(compactionFailedStatus(new JobRunTime(DEFAULT_COMMIT_TIME, DEFAULT_COMMIT_TIME), reasons))
                .build();
    }
}
