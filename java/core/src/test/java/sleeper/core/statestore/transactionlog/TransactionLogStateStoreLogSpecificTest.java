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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore.ThrowingRunnable;
import sleeper.core.util.ExponentialBackoffWithJitter;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class TransactionLogStateStoreLogSpecificTest extends InMemoryTransactionLogStateStoreTestBase {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private StateStore store = stateStore();

    @BeforeEach
    void setUp() throws Exception {
        store.initialise(partitions.buildList());
    }

    @Nested
    @DisplayName("Retry adding transactions")
    class RetryAddTransaction {

        @Test
        void shouldAddTransactionWhenAnotherProcessAddedATransaction() throws Exception {
            // Given
            PartitionTree afterRootSplit = partitions.splitToNewChildren("root", "L", "R", "l").buildTree();
            otherProcess().atomicallyUpdatePartitionAndCreateNewOnes(
                    afterRootSplit.getPartition("root"),
                    afterRootSplit.getPartition("L"), afterRootSplit.getPartition("R"));

            // When
            PartitionTree afterLeftSplit = partitions.splitToNewChildren("L", "LL", "LR", "f").buildTree();
            store.atomicallyUpdatePartitionAndCreateNewOnes(
                    afterLeftSplit.getPartition("L"),
                    afterLeftSplit.getPartition("LL"), afterLeftSplit.getPartition("LR"));

            // Then
            assertThat(new PartitionTree(store.getAllPartitions()))
                    .isEqualTo(afterLeftSplit);
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldRetryAddTransactionWhenAnotherProcessAddedATransactionBetweenUpdateAndAdd() throws Exception {
            // Given
            FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
            FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
            FileReference file3 = fileFactory().rootFile("file3.parquet", 300);
            store.addFile(file1);
            filesLogStore.atStartOfNextAddTransaction(() -> {
                otherProcess().addFile(file2);
            });

            // When
            store.addFile(file3);

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(file1, file2, file3);
            assertThat(retryWaits).hasSize(1);
        }

        @Test
        void shouldRetryAddTransactionWhenConflictOccurredAddingTransaction() throws Exception {
            // Given we cause a transaction conflict by adding another file during an update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReference otherProcessFile = fileFactory().rootFile("other-file.parquet", 100);
            filesLogStore.atStartOfNextAddTransaction(() -> {
                store.addFile(otherProcessFile);
            });

            // When
            store.addFile(file);

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(file, otherProcessFile);
            assertThat(retryWaits).hasSize(1);
        }

        @Test
        void shouldFailAfterTooManyConflictsAddingTransaction() throws Exception {
            // Given we only allow one attempt adding a transaction
            store = stateStore(builder -> builder.maxAddTransactionAttempts(1));
            // And we cause a transaction conflict by adding another file during an update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReference otherProcessFile = fileFactory().rootFile("other-file.parquet", 100);
            filesLogStore.atStartOfNextAddTransaction(() -> {
                store.addFile(otherProcessFile);
            });

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .hasCauseInstanceOf(DuplicateTransactionNumberException.class);
            assertThat(store.getFileReferences())
                    .containsExactly(otherProcessFile);
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldFailIfNoAttemptsConfigured() throws Exception {
            // Given
            store = stateStore(builder -> builder.maxAddTransactionAttempts(0));
            FileReference file = fileFactory().rootFile("file.parquet", 100);

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .cause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("No attempts made");
            assertThat(store.getFileReferences())
                    .isEmpty();
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldFailIfReadingTransactionsFails() throws Exception {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            RuntimeException failure = new RuntimeException("Unexpected failure");
            filesLogStore.atStartOfNextReadTransactions(() -> {
                throw failure;
            });

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .cause().isSameAs(failure);
            assertThat(store.getFileReferences())
                    .isEmpty();
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldFailOnUnexpectedFailureAddingTransaction() throws Exception {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            RuntimeException failure = new RuntimeException("Unexpected failure");
            filesLogStore.atStartOfNextAddTransaction(() -> {
                throw failure;
            });

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .cause().isSameAs(failure);
            assertThat(store.getFileReferences())
                    .isEmpty();
            assertThat(retryWaits).isEmpty();
        }
    }

    @Nested
    @DisplayName("Avoid updating from the transaction log before adding a transaction")
    class NoUpdateFromLogBeforeAddTransaction {

        @Test
        void shouldRetryAddTransactionImmediatelyWhenSetNotToReadFirstAndAnotherProcessAddedATransaction() throws Exception {
            // Given
            FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
            FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
            FileReference file3 = fileFactory().rootFile("file3.parquet", 300);
            store = stateStore(builder -> builder.updateLogBeforeAddTransaction(false));
            store.addFile(file1);
            otherProcess().addFile(file2);

            // When
            store.addFile(file3);

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(file1, file2, file3);
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldRetryAddTransactionTwiceWhenSetNotToReadFirstAndTwoOtherProcessesAddedTransactions() throws Exception {
            // Given
            FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
            FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
            FileReference file3 = fileFactory().rootFile("file3.parquet", 300);
            FileReference file4 = fileFactory().rootFile("file4.parquet", 400);
            store = stateStore(builder -> builder.updateLogBeforeAddTransaction(false));
            store.addFile(file1);
            // File 2 will conflict with the first add transaction call
            otherProcess().addFile(file2);
            // File 3 will conflict with the second add transaction call
            filesLogStore.atStartOfNextAddTransactions(List.of(
                    ThrowingRunnable.DO_NOTHING, () -> {
                        otherProcess().addFile(file3);
                    }));

            // When
            store.addFile(file4);

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(file1, file2, file3, file4);
            assertThat(retryWaits).hasSize(1);
        }

        @Test
        void shouldUpdateLocalStateImmediatelyWhenSetNotToReadFirstAndTransactionOnlyValidatesWithOtherProcessTransaction() throws Exception {
            // Given
            FileReference file = fileFactory().rootFile("test.parquet", 100);
            store = stateStore(builder -> builder.updateLogBeforeAddTransaction(false));
            otherProcess().addFile(file);

            // When
            store.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("test.parquet"))));

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(withJobId("test-job", file));
            assertThat(retryWaits).isEmpty();
        }
    }

    @Nested
    @DisplayName("Exponential backoff for retries")
    class ExponentialBackoffRetries {

        @Test
        void shouldBackoffExponentiallyOnRetries() throws Exception {
            // Given
            store = stateStore(builder -> builder
                    .maxAddTransactionAttempts(TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .retryBackoff(new ExponentialBackoffWithJitter(
                            TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                            constantJitterFraction(0.5), recordWaits(retryWaits))));
            // And we cause a transaction conflict by adding another file during each update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            List<FileReference> otherProcessFiles = IntStream.rangeClosed(1, TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .mapToObj(i -> fileFactory().rootFile("file-" + i + ".parquet", i * 100))
                    .collect(toUnmodifiableList());
            filesLogStore.atStartOfNextAddTransactions(otherProcessFiles.stream()
                    .<ThrowingRunnable>map(otherProcessFile -> () -> otherProcess().addFile(otherProcessFile))
                    .collect(toUnmodifiableList()));

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .hasCauseInstanceOf(DuplicateTransactionNumberException.class);
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(otherProcessFiles);
            assertThat(retryWaits).containsExactly(
                    Duration.parse("PT0.1S"),
                    Duration.parse("PT0.2S"),
                    Duration.parse("PT0.4S"),
                    Duration.parse("PT0.8S"),
                    Duration.parse("PT1.6S"),
                    Duration.parse("PT3.2S"),
                    Duration.parse("PT6.4S"),
                    Duration.parse("PT12.8S"),
                    Duration.parse("PT15S"));
        }

        @Test
        void shouldSkipFirstWaitWhenNotUpdatingLogBeforeAddingTransaction() throws Exception {
            // Given
            store = stateStore(builder -> builder
                    .maxAddTransactionAttempts(TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .retryBackoff(new ExponentialBackoffWithJitter(
                            TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                            constantJitterFraction(0.5), recordWaits(retryWaits)))
                    .updateLogBeforeAddTransaction(false));
            // And we cause a transaction conflict by adding another file during each update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            List<FileReference> otherProcessFiles = IntStream.rangeClosed(1, TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .mapToObj(i -> fileFactory().rootFile("file-" + i + ".parquet", i * 100))
                    .collect(toUnmodifiableList());
            filesLogStore.atStartOfNextAddTransactions(otherProcessFiles.stream()
                    .<ThrowingRunnable>map(otherProcessFile -> () -> otherProcess().addFile(otherProcessFile))
                    .collect(toUnmodifiableList()));

            // When / Then
            assertThatThrownBy(() -> store.addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .hasCauseInstanceOf(DuplicateTransactionNumberException.class);
            assertThat(store.getFileReferences())
                    .containsExactlyInAnyOrderElementsOf(otherProcessFiles);
            assertThat(retryWaits).containsExactly(
                    Duration.parse("PT0.1S"),
                    Duration.parse("PT0.2S"),
                    Duration.parse("PT0.4S"),
                    Duration.parse("PT0.8S"),
                    Duration.parse("PT1.6S"),
                    Duration.parse("PT3.2S"),
                    Duration.parse("PT6.4S"),
                    Duration.parse("PT12.8S"));
        }
    }

    @Nested
    @DisplayName("Ignore empty transactions")
    class IgnoreEmptyTransactions {

        @Test
        void shouldNotAddSplitFileTransactionWithNoRequests() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            SplitFileReferences.from(store).split();

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddSplitFileTransactionWithNoRequestsDirectly() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.splitFileReferences(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddNoFiles() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.addFiles(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddNoFilesWithReferences() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.addFilesWithReferences(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAssignJobIdsWithNoRequests() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.assignJobIds(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAssignJobIdsWithNoFiles() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.assignJobIds(List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(
                    "test-job", "test-partition", List.of())));

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotDeleteNoGCFiles() throws Exception {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.deleteGarbageCollectedFileReferenceCounts(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
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
        TransactionLogStateStore.Builder builder = stateStoreBuilder(schema);
        config.accept(builder);
        return stateStore(builder);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }
}
