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

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStoreTestBase;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TransactionLogStateStoreFollowTransactionsTest extends InMemoryTransactionLogStateStoreTestBase {

    // Tests to add:
    // - Local state has already applied the given transaction (fail or just ignore it?)

    private TransactionLogStateStore committerStore;
    private TransactionLogStateStore followerStore;
    private final AtomicInteger transactionLogReads = new AtomicInteger(0);
    private final List<TransactionLogEntry> transactionEntriesThatWereRead = new ArrayList<>();
    private final Schema schema = schemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(partitions);
        committerStore = (TransactionLogStateStore) super.store;
        followerStore = stateStoreBuilder(schema).build();
    }

    @Test
    void shouldFollowSingleTransaction() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        update(committerStore).addFiles(List.of(file));
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
        update(committerStore).addFiles(List.of(file1));
        TransactionLogEntry entry1 = filesLogStore.getLastEntry();
        update(committerStore).addFiles(List.of(file2));
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
        FileReference file2 = factory.rootFile("file2.parquet", 200L);
        update(committerStore).addFiles(List.of(file1));
        createSnapshots();
        update(committerStore).addFiles(List.of(file2));
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
    void shouldFollowTransactionReadingPreviousSnapshot() {
        // Given 3 files
        FileReference file1 = factory.rootFile("file1.parquet", 100L);
        FileReference file2 = factory.rootFile("file2.parquet", 200L);
        FileReference file3 = factory.rootFile("file3.parquet", 300L);
        // And a snapshot with only file 1
        update(committerStore).addFiles(List.of(file1));
        createSnapshots();
        // And an entry 2 with file 2 as well
        update(committerStore).addFiles(List.of(file2));
        TransactionLogEntry entry2 = filesLogStore.getLastEntry();
        // And a snapshot with file 2 replaced with file 3 (so that this will fail if reapplied on top of the second snapshot)
        update(committerStore).assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("file2.parquet"))));
        TransactionLogEntry entry3 = filesLogStore.getLastEntry();
        update(committerStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(ReplaceFileReferencesRequest.builder()
                .jobId("test-job")
                .inputFiles(List.of("file2.parquet"))
                .newReference(file3)
                .build()));
        TransactionLogEntry entry4 = filesLogStore.getLastEntry();
        createSnapshots();
        trackTransactionLogReads();

        // When we load entry 2 into the follower
        loadNextTransaction(entry2);

        // Then file 2 is added against the first snapshot, rather than the log or the second snapshot
        assertThat(transactionLogReads.get()).isZero();
        // And the replacement with file 3 is applied at query time
        assertThat(followerStore.getFileReferences()).containsExactly(file1, file3);
        assertThat(transactionEntriesThatWereRead).containsExactly(entry3, entry4);
    }

    @Test
    void shouldFailWhenTransactionIsAppliedTwice() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        update(committerStore).addFiles(List.of(file));
        TransactionLogEntry logEntry = filesLogStore.getLastEntry();
        trackTransactionLogReads();
        loadNextTransaction(logEntry);

        // When / Then
        assertThatThrownBy(() -> loadNextTransaction(logEntry))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("Attempted to apply transaction out of order. " +
                        "Last transaction applied was 1, found transaction 1.");
        assertThat(followerStore.getFileReferences()).containsExactly(file);
        assertThat(transactionEntriesThatWereRead).isEmpty();
    }

    @Test
    void shouldFailWhenTransactionsAreAppliedOutOfOrder() {
        // Given
        FileReference file1 = factory.rootFile("file1.parquet", 100L);
        FileReference file2 = factory.rootFile("file2.parquet", 200L);
        update(committerStore).addFiles(List.of(file1));
        TransactionLogEntry entry1 = filesLogStore.getLastEntry();
        update(committerStore).assignJobIds(List.of(AssignJobIdRequest.assignJobOnPartitionToFiles("test-job", "root", List.of("file1.parquet"))));
        TransactionLogEntry entry2 = filesLogStore.getLastEntry();
        update(committerStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(ReplaceFileReferencesRequest.builder()
                .jobId("test-job")
                .inputFiles(List.of("file1.parquet"))
                .newReference(file2)
                .build()));
        TransactionLogEntry entry3 = filesLogStore.getLastEntry();
        loadNextTransaction(entry3);
        trackTransactionLogReads();

        // When / Then
        assertThatThrownBy(() -> loadNextTransaction(entry2))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("Attempted to apply transaction out of order. " +
                        "Last transaction applied was 3, found transaction 2.");
        assertThatThrownBy(() -> loadNextTransaction(entry1))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("Attempted to apply transaction out of order. " +
                        "Last transaction applied was 3, found transaction 1.");
        assertThat(transactionEntriesThatWereRead).isEmpty();
        assertThat(followerStore.getFileReferences()).containsExactly(file2);
    }

    @Test
    void shouldFollowPartitionTransaction() {
        // Given
        TransactionLogEntry entry1 = partitionsLogStore.getLastEntry(); // Initialised in setup
        Partition rootPartition = partitions.buildTree().getRootPartition();
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(committerStore, "root");
        TransactionLogEntry entry2 = partitionsLogStore.getLastEntry();
        trackTransactionLogReads();

        // When
        List<Partition> appliedToPartitions = new ArrayList<>();
        followerStore.applyEntryFromLog(entry2, StateListenerBeforeApply.withPartitionsState(state -> {
            appliedToPartitions.addAll(state.all());
        }));

        // Then
        assertThat(appliedToPartitions).containsExactly(rootPartition);
        assertThat(transactionEntriesThatWereRead).containsExactly(entry1);
        assertThat(new HashSet<>(followerStore.getAllPartitions())).isEqualTo(new HashSet<>(partitions.buildList()));
    }

    private void loadNextTransaction(TransactionLogEntry entry) {
        followerStore.applyEntryFromLog(entry, StateListenerBeforeApply.none());
    }

    private void trackTransactionLogReads() {
        filesLogStore.atStartOfReadTransactions(transactionLogReads::incrementAndGet);
        filesLogStore.onReadTransactionLogEntry(transactionEntriesThatWereRead::add);
        partitionsLogStore.atStartOfReadTransactions(transactionLogReads::incrementAndGet);
        partitionsLogStore.onReadTransactionLogEntry(transactionEntriesThatWereRead::add);
    }
}
