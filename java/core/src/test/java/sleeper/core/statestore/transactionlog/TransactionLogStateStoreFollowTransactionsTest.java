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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class TransactionLogStateStoreFollowTransactionsTest extends InMemoryTransactionLogStateStoreCompactionTrackerTestBase {

    // Tests to add:
    // - Follow partition transactions
    // - Read a snapshot when updating from log
    // - Local state has already applied the given transaction (fail or just ignore it?)

    private TransactionLogStateStore committerStore;
    private TransactionLogStateStore followerStore;
    private final AtomicInteger transactionLogReads = new AtomicInteger(0);
    private final List<TransactionLogEntry> transactionEntriesThatWereRead = new ArrayList<>();

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(schemaWithKey("key", new LongType())).singlePartition("root"));
        committerStore = (TransactionLogStateStore) super.store;
        followerStore = stateStoreBuilder(schemaWithKey("key", new LongType())).build();
    }

    @Test
    void shouldFollowSingleTransaction() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        committerStore.addFiles(List.of(file));
        TransactionLogEntry logEntry = filesLogStore.getLastEntry();
        trackTransactionLogReads();

        // When
        loadNextTransaction(logEntry);

        // Then
        assertThat(followerStore.getFileReferences()).containsExactly(file);
        assertThat(transactionEntriesThatWereRead).isEmpty();
    }

    @Test
    void shouldFollowTransactionReadingPreviousFromLog() {
        // Given
        FileReference file1 = factory.rootFile("file1.parquet", 100L);
        FileReference file2 = factory.rootFile("file2.parquet", 100L);
        committerStore.addFiles(List.of(file1));
        TransactionLogEntry entry1 = filesLogStore.getLastEntry();
        committerStore.addFiles(List.of(file2));
        TransactionLogEntry entry2 = filesLogStore.getLastEntry();
        trackTransactionLogReads();

        // When
        loadNextTransaction(entry2);

        // Then
        assertThat(transactionEntriesThatWereRead).containsExactly(entry1);
        assertThat(followerStore.getFileReferences()).containsExactly(file1, file2);
    }

    @Test
    void shouldFollowTransactionReadingPreviousFromSnapshot() {
        // Given
        FileReference file1 = factory.rootFile("file1.parquet", 100L);
        FileReference file2 = factory.rootFile("file2.parquet", 100L);
        committerStore.addFiles(List.of(file1));
        createSnapshots();
        committerStore.addFiles(List.of(file2));
        TransactionLogEntry entry2 = filesLogStore.getLastEntry();
        trackTransactionLogReads();

        // When
        loadNextTransaction(entry2);

        // Then
        assertThat(transactionLogReads.get()).isZero();
        assertThat(followerStore.getFileReferences()).containsExactly(file1, file2);
        assertThat(transactionEntriesThatWereRead).isEmpty();
    }

    @Test
    @Disabled("TODO")
    void shouldUpdateTransactionLogBasedOnStateStoreProvided() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        committerStore.addFiles(List.of(oldFile));
        committerStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("oldFile"))));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "test-run");
        committerStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("test-run").build()));
        TransactionLogEntry logEntry = filesLogStore.getLastEntry();

        // When
        loadNextTransaction(logEntry);

        // Then
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId()))
                .containsExactly(defaultStatus(trackedJob, defaultCommittedRun(100)));
    }

    private void loadNextTransaction(TransactionLogEntry entry) {
        followerStore.applyEntryFromLog(entry, (e, state) -> {
        });
    }

    private void trackTransactionLogReads() {
        filesLogStore.atStartOfReadTransactions(transactionLogReads::incrementAndGet);
        filesLogStore.onReadTransactionLogEntry(transactionEntriesThatWereRead::add);
    }
}
