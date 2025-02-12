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
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class TransactionLogStateStoreFollowTransactionsTest extends InMemoryTransactionLogStateStoreTestBase {

    // Tests to add:
    // - Follow partition transactions
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
        FileReference file2 = factory.rootFile("file2.parquet", 200L);
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
    void shouldFollowTransactionReadingPreviousSnapshot() {
        // Given 3 files
        FileReference file1 = factory.rootFile("file1.parquet", 100L);
        FileReference file2 = factory.rootFile("file2.parquet", 200L);
        FileReference file3 = factory.rootFile("file3.parquet", 300L);
        // And a snapshot with only file 1
        committerStore.addFiles(List.of(file1));
        createSnapshots();
        // And an entry 2 with file 2 as well
        committerStore.addFiles(List.of(file2));
        TransactionLogEntry entry2 = filesLogStore.getLastEntry();
        // And a snapshot with file 2 replaced with file 3 (so that this will fail if reapplied on top of the second snapshot)
        committerStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("file2.parquet"))));
        TransactionLogEntry entry3 = filesLogStore.getLastEntry();
        committerStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(ReplaceFileReferencesRequest.builder()
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

    private void loadNextTransaction(TransactionLogEntry entry) {
        followerStore.applyEntryFromLog(entry, StateListenerBeforeApply.withState(state -> {
        }));
    }

    private void trackTransactionLogReads() {
        filesLogStore.atStartOfReadTransactions(transactionLogReads::incrementAndGet);
        filesLogStore.onReadTransactionLogEntry(transactionEntriesThatWereRead::add);
    }
}
