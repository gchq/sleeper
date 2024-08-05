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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderRootFirst;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup.SetupStateStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup.setupSnapshotWithFreshState;

public class TransactionLogStateStoreSnapshotsTest extends InMemoryTransactionLogStateStoreTestBase {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilderRootFirst partitions = new PartitionsBuilder(schema).singlePartition("root");

    @Nested
    @DisplayName("Get snapshot on first load")
    class FirstLoad {

        @Test
        void shouldLoadFilesFromSnapshotWhenNotInLogOnFirstLoad() throws Exception {
            // Given
            FileReference file = fileFactory().rootFile(123);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(1, stateStore -> {
                stateStore.addFile(file);
            });

            // Then
            assertThat(stateStore().getFileReferences()).containsExactly(file);
        }

        @Test
        void shouldLoadPartitionsFromSnapshotWhenNotInLogOnFirstLoad() throws Exception {
            // Given
            partitions.splitToNewChildren("root", "L", "R", "abc");

            // When
            createSnapshotWithFreshStateAtTransactionNumber(1, stateStore -> {
                stateStore.initialise(partitions.buildList());
            });

            // Then
            assertThat(stateStore().getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }
    }

    @Nested
    @DisplayName("Only load snapshot when it's a configured number of transactions ahead")
    class RestrictTransactionsAhead {

        @Test
        void shouldNotLoadFilesSnapshotWhenOnlyOneTransactionAheadAfterLoadingLog() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .minTransactionsAheadToLoadSnapshot(2)
                    .timeBetweenSnapshotChecks(Duration.ZERO));
            FileReference logFile = fileFactory().rootFile("log-file.parquet", 123);
            FileReference snapshotFile = fileFactory().rootFile("snapshot-file.parquet", 123);
            stateStore.addFile(logFile);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(2, snapshotStateStore -> {
                snapshotStateStore.addFile(snapshotFile);
            });

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(logFile);
        }

        @Test
        void shouldLoadFilesSnapshotWhenMoreThanConfiguredTransactionsAheadAfterLoadingLog() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .minTransactionsAheadToLoadSnapshot(2)
                    .timeBetweenSnapshotChecks(Duration.ZERO));
            FileReference logFile = fileFactory().rootFile("log-file.parquet", 123);
            FileReference snapshotFile = fileFactory().rootFile("snapshot-file.parquet", 123);
            stateStore.addFile(logFile);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(3, snapshotStateStore -> {
                snapshotStateStore.addFile(snapshotFile);
            });

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(snapshotFile);
        }

        @Test
        void shouldNotLoadPartitionsSnapshotWhenOnlyOneTransactionAheadAfterLoadingLog() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .minTransactionsAheadToLoadSnapshot(2)
                    .timeBetweenSnapshotChecks(Duration.ZERO));
            List<Partition> logPartitions = new PartitionsBuilder(schema).rootFirst("A").buildList();
            List<Partition> snapshotPartitions = new PartitionsBuilder(schema).rootFirst("B").buildList();
            stateStore.initialise(logPartitions);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(2, snapshotStateStore -> {
                snapshotStateStore.initialise(snapshotPartitions);
            });

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyElementsOf(logPartitions);
        }

        @Test
        void shouldLoadPartitionsSnapshotWhenMoreThanConfiguredTransactionsAheadAfterLoadingLog() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .minTransactionsAheadToLoadSnapshot(2)
                    .timeBetweenSnapshotChecks(Duration.ZERO));
            List<Partition> logPartitions = new PartitionsBuilder(schema).rootFirst("A").buildList();
            List<Partition> snapshotPartitions = new PartitionsBuilder(schema).rootFirst("B").buildList();
            stateStore.initialise(logPartitions);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(3, snapshotStateStore -> {
                snapshotStateStore.initialise(snapshotPartitions);
            });

            // Then
            assertThat(stateStore.getAllPartitions()).containsExactlyElementsOf(snapshotPartitions);
        }
    }

    @Nested
    @DisplayName("Only check for new transactions after a configured amount of time")
    class RestrictTransactionCheckFrequency {

        @Test
        void shouldNotCheckForFilesTransactionWhenLessThanConfiguredTimeHasPassed() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .timeBetweenTransactionChecks(Duration.ofMinutes(1))
                    .filesStateUpdateClock(List.of(
                            Instant.parse("2024-05-17T15:15:00Z"), // Check time adding first file
                            Instant.parse("2024-05-17T15:15:01Z"), // Start reading transactions adding first file
                            Instant.parse("2024-05-17T15:15:02Z"), // Finish reading transactions adding first file
                            Instant.parse("2024-05-17T15:15:55Z")) // Check time querying files
                            .iterator()::next));
            FileReference file1 = fileFactory().rootFile("file-1.parquet", 123);
            FileReference file2 = fileFactory().rootFile("file-2.parquet", 456);
            stateStore.addFile(file1);

            // When
            otherProcess().addFile(file2);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file1);
        }

        @Test
        void shouldCheckForFilesTransactionWhenConfiguredTimeHasPassed() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .timeBetweenTransactionChecks(Duration.ofMinutes(1))
                    .filesStateUpdateClock(List.of(
                            Instant.parse("2024-05-17T15:15:00Z"), // Check time adding first file
                            Instant.parse("2024-05-17T15:15:01Z"), // Start reading transactions adding first file
                            Instant.parse("2024-05-17T15:15:02Z"), // Finish reading transactions adding first file
                            Instant.parse("2024-05-17T15:16:05Z"), // Check time querying files
                            Instant.parse("2024-05-17T15:16:06Z"), // Start reading transactions in query
                            Instant.parse("2024-05-17T15:16:07Z")) // Finish reading transactions in query
                            .iterator()::next));
            FileReference file1 = fileFactory().rootFile("file-1.parquet", 123);
            FileReference file2 = fileFactory().rootFile("file-2.parquet", 456);
            stateStore.addFile(file1);

            // When
            otherProcess().addFile(file2);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file1, file2);
        }

        @Test
        void shouldCheckForFilesTransactionWhenAnotherUpdateIsMadeBeforeConfiguredTimeHasPassed() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .timeBetweenTransactionChecks(Duration.ofMinutes(1))
                    .filesStateUpdateClock(List.of(
                            Instant.parse("2024-05-17T15:15:00Z"), // Check time adding file 1
                            Instant.parse("2024-05-17T15:15:01Z"), // Start reading transactions adding file 1
                            Instant.parse("2024-05-17T15:15:02Z"), // Finish reading transactions adding file 1
                            Instant.parse("2024-05-17T15:15:03Z"), // Check time adding file 3
                            Instant.parse("2024-05-17T15:15:04Z"), // Start reading transactions adding file 3
                            Instant.parse("2024-05-17T15:15:05Z"), // Finish reading transactions adding file 3
                            Instant.parse("2024-05-17T15:15:06Z"), // Check time querying files
                            Instant.parse("2024-05-17T15:15:07Z"), // Start reading transactions in query
                            Instant.parse("2024-05-17T15:15:08Z")) // Finish reading transactions in query
                            .iterator()::next));
            FileReference file1 = fileFactory().rootFile("file-1.parquet", 123);
            FileReference file2 = fileFactory().rootFile("file-2.parquet", 456);
            FileReference file3 = fileFactory().rootFile("file-3.parquet", 456);
            stateStore.addFile(file1);

            // When
            otherProcess().addFile(file2);
            stateStore.addFile(file3);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file1, file2, file3);
        }
    }

    @Nested
    @DisplayName("Only check for new snapshots after a configured amount of time")
    class RestrictSnapshotCheckFrequency {

        @Test
        void shouldNotCheckForFilesSnapshotWhenLessThanConfiguredTimeHasPassed() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .minTransactionsAheadToLoadSnapshot(2)
                    .timeBetweenSnapshotChecks(Duration.ofMinutes(1))
                    .filesStateUpdateClock(List.of(
                            Instant.parse("2024-05-17T15:15:00Z"), // Check time adding first file
                            Instant.parse("2024-05-17T15:15:01Z"), // Start reading transactions adding first file
                            Instant.parse("2024-05-17T15:15:02Z"), // Finish reading transactions adding first file
                            Instant.parse("2024-05-17T15:15:55Z"), // Check time querying files
                            Instant.parse("2024-05-17T15:15:56Z"), // Start reading transactions querying files
                            Instant.parse("2024-05-17T15:15:57Z")) // Finish reading transactions querying files
                            .iterator()::next));
            FileReference logFile = fileFactory().rootFile("log-file.parquet", 123);
            FileReference snapshotFile = fileFactory().rootFile("snapshot-file.parquet", 456);
            stateStore.addFile(logFile);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(3, snapshotStateStore -> {
                snapshotStateStore.addFile(snapshotFile);
            });

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(logFile);
        }

        @Test
        void shouldCheckForFilesSnapshotWhenConfiguredTimeHasPassed() throws Exception {
            // Given
            StateStore stateStore = stateStore(builder -> builder
                    .minTransactionsAheadToLoadSnapshot(2)
                    .timeBetweenSnapshotChecks(Duration.ofMinutes(1))
                    .filesStateUpdateClock(List.of(
                            Instant.parse("2024-05-17T15:15:00Z"), // Check time adding first file
                            Instant.parse("2024-05-17T15:15:01Z"), // Start reading transactions adding first file
                            Instant.parse("2024-05-17T15:15:02Z"), // Finish reading transactions adding first file
                            Instant.parse("2024-05-17T15:16:05Z"), // Check time querying files
                            Instant.parse("2024-05-17T15:16:06Z"), // Start reading transactions querying files
                            Instant.parse("2024-05-17T15:16:07Z")) // Finish reading transactions querying files
                            .iterator()::next));
            FileReference logFile = fileFactory().rootFile("log-file.parquet", 123);
            FileReference snapshotFile = fileFactory().rootFile("snapshot-file.parquet", 456);
            stateStore.addFile(logFile);

            // When
            createSnapshotWithFreshStateAtTransactionNumber(3, snapshotStateStore -> {
                snapshotStateStore.addFile(snapshotFile);
            });

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(snapshotFile);
        }
    }

    private StateStore otherProcess() {
        return stateStore();
    }

    private StateStore stateStore() {
        return stateStore(builder -> {
        });
    }

    private StateStore stateStore(Consumer<TransactionLogStateStore.Builder> config) {
        TransactionLogStateStore.Builder builder = stateStoreBuilder(schema)
                .minTransactionsAheadToLoadSnapshot(1);
        config.accept(builder);
        return stateStore(builder);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    protected void createSnapshotWithFreshStateAtTransactionNumber(
            long transactionNumber, SetupStateStore setupState) throws Exception {
        InMemoryTransactionLogSnapshotSetup snapshotSetup = setupSnapshotWithFreshState(sleeperTable, schema, setupState);
        fileSnapshots.setLatestSnapshot(snapshotSetup.createFilesSnapshot(transactionNumber));
        partitionSnapshots.setLatestSnapshot(snapshotSetup.createPartitionsSnapshot(transactionNumber));
    }

}
