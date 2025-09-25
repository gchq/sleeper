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
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStoreTestBase;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStore.ThrowingRunnable;
import sleeper.core.statestore.transactionlog.log.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.ADD_TRANSACTION_MAX_ATTEMPTS;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TransactionLogStateStoreLogSpecificTest extends InMemoryTransactionLogStateStoreTestBase {

    private final Schema schema = createSchemaWithKey("key", new StringType());

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
            update(otherProcess()).atomicallyUpdatePartitionAndCreateNewOnes(
                    afterRootSplit.getPartition("root"),
                    afterRootSplit.getPartition("L"), afterRootSplit.getPartition("R"));

            // When
            PartitionTree afterLeftSplit = partitions.splitToNewChildren("L", "LL", "LR", "f").buildTree();
            update(store).atomicallyUpdatePartitionAndCreateNewOnes(
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
            update(store).addFile(file1);
            filesLogStore.atStartOfNextAddTransaction(() -> {
                update(otherProcess()).addFile(file2);
            });

            // When
            update(store).addFile(file3);

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
                update(store).addFile(otherProcessFile);
            });

            // When
            update(store).addFile(file);

            // Then
            assertThat(store.getFileReferences())
                    .containsExactly(file, otherProcessFile);
            assertThat(retryWaits).hasSize(1);
        }

        @Test
        void shouldFailAfterTooManyConflictsAddingTransaction() {
            // Given we only allow one attempt adding a transaction
            tableProperties.setNumber(ADD_TRANSACTION_MAX_ATTEMPTS, 1);
            store = stateStore();
            // And we cause a transaction conflict by adding another file during an update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReference otherProcessFile = fileFactory().rootFile("other-file.parquet", 100);
            filesLogStore.atStartOfNextAddTransaction(() -> {
                update(store).addFile(otherProcessFile);
            });

            // When / Then
            assertThatThrownBy(() -> update(store).addFile(file))
                    .isInstanceOf(StateStoreException.class)
                    .hasCauseInstanceOf(DuplicateTransactionNumberException.class);
            assertThat(store.getFileReferences())
                    .containsExactly(otherProcessFile);
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldFailIfNoAttemptsConfigured() {
            // Given
            tableProperties.setNumber(ADD_TRANSACTION_MAX_ATTEMPTS, 0);
            store = stateStore();
            FileReference file = fileFactory().rootFile("file.parquet", 100);

            // When / Then
            assertThatThrownBy(() -> update(store).addFile(file))
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
            assertThatThrownBy(() -> update(store).addFile(file))
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
            assertThatThrownBy(() -> update(store).addFile(file))
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
            update(store).addFile(file1);
            update(otherProcess()).addFile(file2);

            // When
            update(store).addFile(file3);

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
            update(store).addFile(file1);
            // File 2 will conflict with the first add transaction call
            update(otherProcess()).addFile(file2);
            // File 3 will conflict with the second add transaction call
            filesLogStore.atStartOfNextAddTransactions(List.of(
                    ThrowingRunnable.DO_NOTHING, () -> {
                        update(otherProcess()).addFile(file3);
                    }));

            // When
            update(store).addFile(file4);

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
            update(otherProcess()).addFile(file);

            // When
            update(store).assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("test.parquet"))));

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
            // Given we cause a transaction conflict by adding another file during each update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            List<FileReference> otherProcessFiles = IntStream.rangeClosed(1, TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .mapToObj(i -> fileFactory().rootFile("file-" + i + ".parquet", i * 100))
                    .collect(toUnmodifiableList());
            filesLogStore.atStartOfNextAddTransactions(otherProcessFiles.stream()
                    .<ThrowingRunnable>map(otherProcessFile -> () -> update(otherProcess()).addFile(otherProcessFile))
                    .collect(toUnmodifiableList()));

            // When / Then
            assertThatThrownBy(() -> update(store).addFile(file))
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
                    .updateLogBeforeAddTransaction(false));
            // And we cause a transaction conflict by adding another file during each update
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            List<FileReference> otherProcessFiles = IntStream.rangeClosed(1, TransactionLogStateStore.DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS)
                    .mapToObj(i -> fileFactory().rootFile("file-" + i + ".parquet", i * 100))
                    .collect(toUnmodifiableList());
            filesLogStore.atStartOfNextAddTransactions(otherProcessFiles.stream()
                    .<ThrowingRunnable>map(otherProcessFile -> () -> update(otherProcess()).addFile(otherProcessFile))
                    .collect(toUnmodifiableList()));

            // When / Then
            assertThatThrownBy(() -> update(store).addFile(file))
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
            update(store).splitFileReferences(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddNoFiles() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            update(store).addFiles(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAddNoFilesWithReferences() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            update(store).addFilesWithReferences(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAssignJobIdsWithNoRequests() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            update(store).assignJobIds(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotAssignJobIdsWithNoFiles() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            update(store).assignJobIds(List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(
                    "test-job", "test-partition", List.of())));

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotReplaceNoFileReferences() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            update(store).atomicallyReplaceFileReferencesWithNewOnes(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }

        @Test
        void shouldNotDeleteNoGCFiles() {
            // Given
            long transactionNumberBefore = filesLogStore.getLastTransactionNumber();

            // When
            update(store).deleteGarbageCollectedFileReferenceCounts(List.of());

            // Then
            assertThat(filesLogStore.getLastTransactionNumber()).isEqualTo(transactionNumberBefore);
        }
    }

    @Nested
    @DisplayName("Store the body of a transaction before adding to the log")
    class StoreTransactionBodySeparately {

        @Test
        void shouldAddFileTransactionWhoseBodyIsHeldInS3() {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReferenceTransaction transaction = AddFilesTransaction.fromReferences(List.of(file));
            String key = "table/transactions/myTransaction.json";
            transactionBodyStore.store(key, tableId, transaction);

            // When
            store.addTransaction(AddTransactionRequest.withTransaction(transaction).bodyKey(key).build());

            // Then
            assertThat(otherProcess().getFileReferences()).containsExactly(file);
        }

        @Test
        void shouldAddPartitionTransactionWhoseBodyIsHeldInS3() {
            // Given
            PartitionTree tree = partitions.splitToNewChildren("root", "L", "R", "m").buildTree();
            PartitionTransaction transaction = new InitialisePartitionsTransaction(tree.getAllPartitions());
            String key = "table/transactions/myTransaction.json";
            transactionBodyStore.store(key, tableId, transaction);

            // When
            store.addTransaction(AddTransactionRequest.withTransaction(transaction).bodyKey(key).build());

            // Then
            assertThat(otherProcess().getAllPartitions()).isEqualTo(tree.getAllPartitions());
        }

        @Test
        void shouldFailToLoadTransactionIfBodyIsNotInBodyStore() {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReferenceTransaction transaction = AddFilesTransaction.fromReferences(List.of(file));
            String key = "table/transactions/myTransaction.json";
            store.addTransaction(AddTransactionRequest.withTransaction(transaction).bodyKey(key).build());

            // When / Then
            assertThatThrownBy(() -> otherProcess().getFileReferences())
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Failed updating state from transactions");
        }

        @Test
        void shouldFailToLoadTransactionIfTypeHeldInLogDoesNotMatchTypeInBodyStore() {
            // Given
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            FileReferenceTransaction transactionInStore = AddFilesTransaction.fromReferences(List.of(file));
            FileReferenceTransaction transactionInLog = new ClearFilesTransaction();
            String key = "table/transactions/myTransaction.json";
            transactionBodyStore.store(key, tableId, transactionInStore);
            store.addTransaction(AddTransactionRequest.withTransaction(transactionInLog).bodyKey(key).build());

            // When / Then
            assertThatThrownBy(() -> otherProcess().getFileReferences())
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Failed updating state from transactions");
        }
    }

    @Nested
    @DisplayName("Store too big transactions")
    class StoreBigTransactions {

        @Test
        void shouldStoreTransactionInBodyStoreWhenTooBig() {
            // Given the body store determines that the transaction is too big for the log
            transactionBodyStore.setStoreTransactionsWithObjectKeys(List.of("test/object"));
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            AddFilesTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));

            // When we add the transaction
            store.addTransaction(AddTransactionRequest.withTransaction(transaction).build());

            // Then the log points to the transaction in the body store
            assertThat(filesLogStore.getLastEntry())
                    .isEqualTo(new TransactionLogEntry(1, DEFAULT_UPDATE_TIME, TransactionType.ADD_FILES, "test/object"));
            assertThat(transactionBodyStore.<AddFilesTransaction>getBody("test/object", tableId, TransactionType.ADD_FILES))
                    .isEqualTo(transaction);
        }

        @Test
        void shouldNotCopyTransactionInBodyStoreWhenAlreadyPresent() {
            // Given the body store determines that the transaction is too big for the log
            transactionBodyStore.setStoreTransactionsWithObjectKeys(List.of());
            FileReference file = fileFactory().rootFile("file.parquet", 100);
            AddFilesTransaction transaction = new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file)));
            // And we upload the transaction to the body store
            transactionBodyStore.store("test/object", tableId, transaction);

            // When we add the transaction
            store.addTransaction(AddTransactionRequest.withTransaction(transaction).bodyKey("test/object").build());

            // Then the log points to the transaction in the body store
            assertThat(filesLogStore.getLastEntry())
                    .isEqualTo(new TransactionLogEntry(1, DEFAULT_UPDATE_TIME, TransactionType.ADD_FILES, "test/object"));
            assertThat(transactionBodyStore.<AddFilesTransaction>getBody("test/object", tableId, TransactionType.ADD_FILES))
                    .isEqualTo(transaction);
        }

        @Test
        void shouldStoreOnceWhenAddTransactionIsRetried() {
            // Given we have 3 files
            FileReference file1 = fileFactory().rootFile("file1.parquet", 100);
            FileReference file2 = fileFactory().rootFile("file2.parquet", 200);
            FileReference file3 = fileFactory().rootFile("file3.parquet", 300);
            // And we will store 3 transactions in the body store
            transactionBodyStore.setStoreTransactionsWithObjectKeys(List.of("object/1", "object/2", "object/3"));
            // And we add one file now
            update(store).addFile(file1);
            // And we add one after the local state is updated but before the next transaction is added to the log
            filesLogStore.atStartOfNextAddTransaction(() -> {
                update(otherProcess()).addFile(file2);
            });

            // When we add the third file
            update(store).addFile(file3);

            // Then all 3 files are present, with one retry
            assertThat(store.getFileReferences())
                    .containsExactly(file1, file2, file3);
            assertThat(retryWaits).hasSize(1);
            // And 3 transactions were written to the log
            assertThat(filesLogStore.readTransactions(TransactionLogRange.fromMinimum(1))).containsExactly(
                    new TransactionLogEntry(1, DEFAULT_UPDATE_TIME, TransactionType.ADD_FILES, "object/1"),
                    new TransactionLogEntry(2, DEFAULT_UPDATE_TIME, TransactionType.ADD_FILES, "object/3"),
                    new TransactionLogEntry(3, DEFAULT_UPDATE_TIME, TransactionType.ADD_FILES, "object/2"));
            // And 3 transactions were written to the body store before they were added to the log
            assertThat(Stream.of("object/1", "object/2", "object/3")
                    .map(key -> transactionBodyStore.getBody(key, tableId, TransactionType.ADD_FILES)))
                    .containsExactly(
                            new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1))),
                            new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file3))),
                            new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2))));
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
