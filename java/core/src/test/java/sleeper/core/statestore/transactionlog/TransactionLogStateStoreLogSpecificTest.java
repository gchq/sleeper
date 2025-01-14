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
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore.ThrowingRunnable;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ThreadSleepTestHelper;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.AFTER_DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.splitFile;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;

public class TransactionLogStateStoreLogSpecificTest extends InMemoryTransactionLogStateStoreTestBase {

    private final Schema schema = schemaWithKey("key", new StringType());

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(schema).singlePartition("root"));
    }

    @Nested
    @DisplayName("Retry adding transactions")
    class RetryAddTransaction {

        @Test
        void shouldAddTransactionWhenAnotherProcessAddedATransaction() {
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
        void shouldRetryAddTransactionWhenAnotherProcessAddedATransactionBetweenUpdateAndAdd() {
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
        void shouldRetryAddTransactionWhenConflictOccurredAddingTransaction() {
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
        void shouldFailAfterTooManyConflictsAddingTransaction() {
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
        void shouldFailIfNoAttemptsConfigured() {
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
        void shouldFailIfReadingTransactionsFails() {
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
        void shouldFailOnUnexpectedFailureAddingTransaction() {
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
        void shouldRetryAddTransactionImmediatelyWhenSetNotToReadFirstAndAnotherProcessAddedATransaction() {
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
        void shouldRetryAddTransactionTwiceWhenSetNotToReadFirstAndTwoOtherProcessesAddedTransactions() {
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
        void shouldUpdateLocalStateImmediatelyWhenSetNotToReadFirstAndTransactionOnlyValidatesWithOtherProcessTransaction() {
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
        void shouldBackoffExponentiallyOnRetries() {
            // Given
            store = stateStore(builder -> builder
                    .maxAddTransactionAttempts(TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .retryBackoff(new ExponentialBackoffWithJitter(
                            TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                            constantJitterFraction(0.5), ThreadSleepTestHelper.recordWaits(retryWaits))));
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
        void shouldSkipFirstWaitWhenNotUpdatingLogBeforeAddingTransaction() {
            // Given
            store = stateStore(builder -> builder
                    .maxAddTransactionAttempts(TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .retryBackoff(new ExponentialBackoffWithJitter(
                            TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                            constantJitterFraction(0.5), ThreadSleepTestHelper.recordWaits(retryWaits)))
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
        void shouldNotAddSplitFileTransactionWithNoRequests() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            SplitFileReferences.from(store).split();

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddSplitFileTransactionWithNoRequestsDirectly() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.splitFileReferences(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddNoFiles() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.addFiles(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddNoFilesWithReferences() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.addFilesWithReferences(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAssignJobIdsWithNoRequests() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.assignJobIds(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAssignJobIdsWithNoFiles() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.assignJobIds(List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(
                    "test-job", "test-partition", List.of())));

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotDeleteNoGCFiles() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            store.deleteGarbageCollectedFileReferenceCounts(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }
    }

    @Nested
    @DisplayName("Store the body of a transaction before adding to the log")
    class StoreTransactionBodySeparately {

        private TransactionLogStateStore store;

        @BeforeEach
        void setUp() {
            store = (TransactionLogStateStore) TransactionLogStateStoreLogSpecificTest.this.store;
        }

        @Test
        void shouldAddFileTransactionWhoseBodyIsHeldInS3() {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReferenceTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));
            String key = "table/transactions/myTransaction.json";
            transactionBodyStore.store(key, transaction);

            // When
            store.addTransaction(AddTransactionRequest.transactionInBucket(key, transaction));

            // Then
            assertThat(otherProcess().getFileReferences()).containsExactly(file);
        }

        @Test
        void shouldAddPartitionTransactionWhoseBodyIsHeldInS3() {
            // Given
            PartitionTree tree = partitions.splitToNewChildren("root", "L", "R", "m").buildTree();
            PartitionTransaction transaction = new InitialisePartitionsTransaction(tree.getAllPartitions());
            String key = "table/transactions/myTransaction.json";
            transactionBodyStore.store(key, transaction);

            // When
            store.addTransaction(AddTransactionRequest.transactionInBucket(key, transaction));

            // Then
            assertThat(otherProcess().getAllPartitions()).isEqualTo(tree.getAllPartitions());
        }

        @Test
        void shouldFailToLoadTransactionIfBodyIsNotInBodyStore() {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReferenceTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));
            String key = "table/transactions/myTransaction.json";
            store.addTransaction(AddTransactionRequest.transactionInBucket(key, transaction));

            // When / Then
            assertThatThrownBy(() -> otherProcess().getFileReferences())
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Failed updating state from transactions");
        }

        @Test
        void shouldFailToLoadTransactionIfTypeHeldInLogDoesNotMatchTypeInBodyStore() {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReferenceTransaction transactionInStore = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));
            FileReferenceTransaction transactionInLog = new ClearFilesTransaction();
            String key = "table/transactions/myTransaction.json";
            transactionBodyStore.store(key, transactionInStore);
            store.addTransaction(AddTransactionRequest.transactionInBucket(key, transactionInLog));

            // When / Then
            assertThatThrownBy(() -> otherProcess().getFileReferences())
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Failed updating state from transactions");
        }
    }

    @Nested
    @DisplayName("Apply a batch of compaction commits, discarding but tracking failures")
    class ApplyCompactionCommitBatch {

        private TransactionLogStateStore store;

        @BeforeEach
        void setUp() {
            store = (TransactionLogStateStore) TransactionLogStateStoreLogSpecificTest.this.store;
        }

        @Test
        public void shouldCommitCompaction() {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "root", List.of("oldFile"))));
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("oldFile"), newFile)));

            // When
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).containsExactly(newFile);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(newFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("oldFile");
            assertThat(store.getPartitionToReferencedFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files -> assertThat(files).containsExactly("newFile"));
        }

        @Test
        void shouldIgnoreCompactionCommitWhenAlreadyCommitted() {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "root", List.of("oldFile"))));
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("oldFile"), newFile)));
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // When
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).containsExactly(newFile);
            assertThat(store.getFileReferencesWithNoJobId()).containsExactly(newFile);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME))
                    .containsExactly("oldFile");
            assertThat(store.getPartitionToReferencedFilesMap())
                    .containsOnlyKeys("root")
                    .hasEntrySatisfying("root", files -> assertThat(files).containsExactly("newFile"));
        }

        @Test
        void shouldIgnoreCompactionCommitWhenInputFilesAreNotAssignedToJob() {
            // Given
            FileReference oldFile = factory.rootFile("oldFile", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile);
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("oldFile"), newFile)));

            // When
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).containsExactly(oldFile);
        }

        @Test
        public void shouldIgnoreCompactionCommitWhenInputFileIsNotInStateStore() {
            // Given
            FileReference newFile = factory.rootFile("newFile", 100L);
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("oldFile"), newFile)));

            // When we commit a compaction with an input file that is not in the state store, e.g. because the
            // compaction has already been committed, and the file has already been garbage collected.
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        public void shouldIgnoreCompactionCommitWhenOneInputFileIsNotInStateStore() {
            // Given
            FileReference oldFile1 = factory.rootFile("oldFile1", 100L);
            FileReference newFile = factory.rootFile("newFile", 100L);
            store.addFile(oldFile1);
            store.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("job1", "root", List.of("oldFile1"))));
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("oldFile1", "oldFile2"), newFile)));

            // When
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).containsExactly(withJobId("job1", oldFile1));
            assertThat(store.getFileReferencesWithNoJobId()).isEmpty();
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
        }

        @Test
        public void shouldIgnoreCompactionCommitWhenFileReferenceDoesNotExistInPartition() {
            // Given
            splitPartition("root", "L", "R", "5");
            FileReference file = factory.rootFile("file", 100L);
            FileReference existingReference = splitFile(file, "L");
            store.addFile(existingReference);
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("file"), factory.rootFile("file2", 100L))));

            // When
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).containsExactly(existingReference);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
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
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                    replaceJobFileReferences("job1", List.of("oldFile"), newReference)));

            // When
            store.addTransaction(AddTransactionRequest.transaction(transaction));

            // Then
            assertThat(store.getFileReferences()).containsExactlyInAnyOrder(
                    withJobId("job1", existingReference), newReference);
            assertThat(store.getReadyForGCFilenamesBefore(AFTER_DEFAULT_UPDATE_TIME)).isEmpty();
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
