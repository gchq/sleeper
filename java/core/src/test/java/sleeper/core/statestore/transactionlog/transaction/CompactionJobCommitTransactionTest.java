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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStoreCompactionTrackerTestBase;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.AFTER_DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class CompactionJobCommitTransactionTest extends InMemoryTransactionLogStateStoreCompactionTrackerTestBase {

    private TransactionLogStateStore committerStore;
    private TransactionLogStateStore followerStore;

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(createSchemaWithKey("key", new LongType())).singlePartition("root"));
        committerStore = (TransactionLogStateStore) super.store;
        followerStore = stateStoreBuilder(createSchemaWithKey("key", new LongType())).build();
    }

    @Test
    public void shouldCommitCompaction() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        update(committerStore).addFiles(List.of(oldFile));
        update(committerStore).assignJobId("job1", "root", List.of("oldFile"));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "test-run");

        // When
        addTransactionWithTracking(new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("test-run").build())));

        // Then
        FileReference committedNewFile = newFile.toBuilder().lastStateStoreUpdateTime(DEFAULT_COMMIT_TIME).build();
        assertThat(followerStore.getFileReferences()).containsExactly(committedNewFile);
        assertThat(followerStore.getFileReferencesWithNoJobId()).containsExactly(committedNewFile);
        assertThat(followerStore.getReadyForGCFilenamesBefore(DEFAULT_COMMIT_TIME.plus(Duration.ofMinutes(1))))
                .containsExactly("oldFile");
        assertThat(followerStore.getPartitionToReferencedFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files -> assertThat(files).containsExactly("newFile"));
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId()))
                .containsExactly(defaultStatus(trackedJob, defaultCommittedRun(100)));
    }

    @Test
    void shouldFailWhenAlreadyCommitted() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        update(committerStore).addFiles(List.of(oldFile));
        update(committerStore).assignJobIds(List.of(
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
        FileReference committedNewFile = newFile.toBuilder().lastStateStoreUpdateTime(DEFAULT_COMMIT_TIME).build();
        assertThat(followerStore.getFileReferences()).containsExactly(committedNewFile);
        assertThat(followerStore.getFileReferencesWithNoJobId()).containsExactly(committedNewFile);
        assertThat(followerStore.getReadyForGCFilenamesBefore(DEFAULT_COMMIT_TIME.plus(Duration.ofMinutes(1))))
                .containsExactly("oldFile");
        assertThat(followerStore.getPartitionToReferencedFilesMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files -> assertThat(files).containsExactly("newFile"));
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob,
                        defaultCommittedRun(100),
                        defaultFailedCommitRun(100, List.of("File reference not found in partition root, filename oldFile"))));
    }

    @Test
    void shouldFailWhenInputFilesAreNotAssignedToJob() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        update(committerStore).addFile(oldFile);
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(oldFile);
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100,
                        List.of("Reference to file is not assigned to job job1, in partition root, filename oldFile"))));
    }

    @Test
    public void shouldFailWhenInputFileIsNotInStateStore() {
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
        assertThat(followerStore.getFileReferences()).isEmpty();
        assertThat(followerStore.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100, List.of("File not found: oldFile"))));
    }

    @Test
    public void shouldFailWhenOneInputFileIsNotInStateStore() {
        // Given
        FileReference oldFile1 = factory.rootFile("oldFile1", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        update(committerStore).addFile(oldFile1);
        update(committerStore).assignJobId("job1", "root", List.of("oldFile1"));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 2);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile1", "oldFile2"), newFile).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(withJobId("job1", oldFile1));
        assertThat(followerStore.getFileReferencesWithNoJobId()).isEmpty();
        assertThat(followerStore.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100, List.of("File not found: oldFile2"))));
    }

    @Test
    public void shouldFailWhenFileReferenceDoesNotExistInPartition() {
        // Given
        splitPartition("root", "L", "R", 5);
        FileReference file = factory.rootFile("file", 100L);
        FileReference existingReference = splitFile(file, "L");
        update(committerStore).addFile(existingReference);
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("file"), factory.rootFile("file2", 100L)).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(existingReference);
        assertThat(followerStore.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100,
                        List.of("File reference not found in partition root, filename file"))));
    }

    @Test
    void shouldFailWhenFileToBeMarkedReadyForGCHasSameFileNameAsNewFile() {
        // Given
        FileReference file = factory.rootFile("file1", 100L);
        update(committerStore).addFile(file);
        update(committerStore).assignJobId("job1", "root", List.of("file1"));

        // When / Then
        assertThatThrownBy(() -> new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferences("job1", List.of("file1"), file))))
                .isInstanceOf(ReplaceRequestsFailedException.class)
                .cause().isInstanceOf(NewReferenceSameAsOldReferenceException.class);
    }

    @Test
    public void shouldFailWhenOutputFileAlreadyExists() {
        // Given
        splitPartition("root", "L", "R", 5);
        FileReference file = factory.rootFile("oldFile", 100L);
        FileReference existingReference = splitFile(file, "L");
        FileReference newReference = factory.partitionFile("L", "newFile", 100L);
        update(committerStore).addFiles(List.of(existingReference, newReference));
        update(committerStore).assignJobId("job1", "L", List.of("oldFile"));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "run1");
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newReference).jobRunId("run1").build()));

        // When
        addTransactionWithTracking(transaction);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactlyInAnyOrder(
                withJobId("job1", existingReference), newReference);
        assertThat(followerStore.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId())).containsExactly(
                defaultStatus(trackedJob, defaultFailedCommitRun(100,
                        List.of("File already exists: newFile"))));
    }

    @Test
    void shouldFailWhenOtherProcessMakesConflictingSynchronousCommitOfSameJob() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        update(committerStore).addFiles(List.of(oldFile));
        update(committerStore).assignJobId("job1", "root", List.of("oldFile"));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "test-run");
        trackJobRun(trackedJob, "other-run");
        ReplaceFileReferencesRequest.Builder requestBuilder = replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile);
        ReplaceFileReferencesTransaction testTransaction = new ReplaceFileReferencesTransaction(List.of(
                requestBuilder.jobRunId("test-run").build()));
        ReplaceFileReferencesTransaction otherTransaction = new ReplaceFileReferencesTransaction(List.of(
                requestBuilder.jobRunId("other-run").build()));
        filesLogStore.atStartOfNextAddTransaction(() -> {
            otherTransaction.synchronousCommit(store);
        });

        // When / Then
        assertThatThrownBy(() -> testTransaction.synchronousCommit(store))
                .isInstanceOf(ReplaceRequestsFailedException.class)
                .cause().isInstanceOf(FileReferenceNotFoundException.class);
        assertThat(followerStore.getFileReferences()).containsExactly(newFile);
    }

    private void addTransactionWithTracking(ReplaceFileReferencesTransaction transaction) {
        // Transaction is added in a committer process
        committerStore.fixFileUpdateTime(DEFAULT_COMMIT_TIME);
        committerStore.addTransaction(AddTransactionRequest.withTransaction(transaction).build());

        // Job tracker updates are done in a separate process that reads from the log and updates its local state
        TransactionLogEntry entry = filesLogStore.getLastEntry();
        followerStore.applyEntryFromLog(entry, StateListenerBeforeApply.updateTrackers(sleeperTable, IngestJobTracker.NONE, tracker));
    }
}
