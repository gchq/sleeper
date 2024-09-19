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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.SplitPartitionTransaction;
import sleeper.core.table.TableStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogTransactionDeleterTest {
    private final Schema schema = schemaWithKey("key", new StringType());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final TableStatus tableStatus = tableProperties.getStatus();
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("root");
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final InMemoryTransactionLogStore filesLogStore = transactionLogs.getFilesLogStore();
    private final InMemoryTransactionLogStore partitionsLogStore = transactionLogs.getPartitionsLogStore();
    private final StateStore stateStore = transactionLogs.stateStoreBuilder(tableStatus, schema).build();
    private final InMemoryTransactionLogSnapshotMetadataStore snapshots = new InMemoryTransactionLogSnapshotMetadataStore();

    @Test
    void shouldDeleteOldTransactionWhenTwoAreBeforeLatestSnapshot() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we have a snapshot at the head of the file log
        Instant snapshotTime = Instant.parse("2024-06-24T15:46:30Z");
        snapshots.addFilesSnapshotAt(2, snapshotTime);
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 1);

        // When we delete transactions long enough after the snapshot
        deleteOldTransactionsAt(snapshotTime.plus(Duration.ofMinutes(2)));

        // Then the transaction is deleted
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldNotDeleteOldTransactionWhenNoSnapshotIsOldEnough() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:45Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we have a snapshot at the head of the file log
        Instant snapshotTime = Instant.parse("2024-06-24T15:46:30Z");
        snapshots.addFilesSnapshotAt(2, snapshotTime);
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 2);

        // When we delete transactions soon after the snapshot
        deleteOldTransactionsAt(snapshotTime.plus(Duration.ofMinutes(1)));

        // Then nothing is deleted
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(
                        new TransactionLogEntry(1, Instant.parse("2024-06-24T15:45:45Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))),
                        new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldDeleteOldTransactionWhenOldSnapshotIsOldEnough() throws Exception {
        // Given we have three file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        FileReference file3 = fileFactory.rootFile("file3.parquet", 789L);
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        setupAtTime(Instant.parse("2024-06-24T15:47:00Z"), () -> stateStore.addFile(file3));
        // And we have two snapshots
        Instant snapshotTime1 = Instant.parse("2024-06-24T15:46:30Z");
        Instant snapshotTime2 = Instant.parse("2024-06-24T15:47:30Z");
        snapshots.addFilesSnapshotAt(2, snapshotTime1);
        snapshots.addFilesSnapshotAt(3, snapshotTime2);
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 1);

        // When we delete transactions soon after the second snapshot, but long enough after the first snapshot
        deleteOldTransactionsAt(snapshotTime2.plus(Duration.ofSeconds(50)));

        // Then a transaction is deleted behind the first snapshot
        assertThat(filesLogStore.readTransactionsAfter(0)).containsExactly(
                new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))),
                new TransactionLogEntry(3, Instant.parse("2024-06-24T15:47:00Z"),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file3)))));
    }

    @Test
    void shouldNotDeleteOldTransactionWhenSnapshotIsOldEnoughButTransactionNotFarEnoughBehind() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we have a snapshot at the head of the file log
        Instant snapshotTime = Instant.parse("2024-06-24T15:46:30Z");
        snapshots.addFilesSnapshotAt(2, snapshotTime);
        // And we configure to delete any transactions more than two before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 2);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 1);

        // When we delete transactions long enough after the snapshot
        deleteOldTransactionsAt(snapshotTime.plus(Duration.ofMinutes(2)));

        // Then nothing is deleted
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(
                        new TransactionLogEntry(1, Instant.parse("2024-06-24T15:45:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))),
                        new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldNotDeleteTransactionsWhenNoSnapshotExistsYet() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:45Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 1);

        // When we delete transactions long after the transactions
        deleteOldTransactionsAt(Instant.parse("2024-06-25T02:00:00Z"));

        // Then nothing is deleted
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(
                        new TransactionLogEntry(1, Instant.parse("2024-06-24T15:45:45Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))),
                        new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldDeleteOldPartitionTransactionWhenTwoAreBeforeLatestSnapshot() throws Exception {
        // Given we have two partitions transactions
        PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("root");
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.initialise(partitions.buildList()));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> partitions
                .splitToNewChildren("root", "L", "R", "m")
                .applySplit(stateStore, "root"));
        // And we have a snapshot at the head of the partitions log
        Instant snapshotTime = Instant.parse("2024-06-24T15:46:30Z");
        snapshots.addPartitionsSnapshotAt(2, snapshotTime);
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 1);

        // When
        deleteOldTransactionsAt(snapshotTime.plus(Duration.ofMinutes(2)));

        // Then
        PartitionTree partitionTree = partitions.buildTree();
        assertThat(partitionsLogStore.readTransactionsAfter(0))
                .containsExactly(new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new SplitPartitionTransaction(partitionTree.getRootPartition(), List.of(
                                partitionTree.getPartition("L"),
                                partitionTree.getPartition("R")))));
    }

    @Test
    void shouldDeleteOldTransactionWhenTwoSnapshotsAreOldEnough() throws Exception {
        // Given we have three file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        FileReference file3 = fileFactory.rootFile("file3.parquet", 789L);
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        setupAtTime(Instant.parse("2024-06-24T15:47:00Z"), () -> stateStore.addFile(file3));
        // And we have two snapshots
        Instant snapshotTime1 = Instant.parse("2024-06-24T15:46:10Z");
        Instant snapshotTime2 = Instant.parse("2024-06-24T15:46:30Z");
        Instant snapshotTime3 = Instant.parse("2024-06-24T15:47:30Z");
        snapshots.addFilesSnapshotAt(1, snapshotTime1);
        snapshots.addFilesSnapshotAt(2, snapshotTime2);
        snapshots.addFilesSnapshotAt(3, snapshotTime3);
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 1);

        // When we delete transactions soon after the third snapshot, but long enough after the second and first snapshot
        deleteOldTransactionsAt(snapshotTime3.plus(Duration.ofSeconds(50)));

        // Then transactions are deleted behind the second snapshot
        assertThat(filesLogStore.readTransactionsAfter(0)).containsExactly(
                new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))),
                new TransactionLogEntry(3, Instant.parse("2024-06-24T15:47:00Z"),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file3)))));
    }

    private void setupAtTime(Instant time, SetupFunction setup) throws Exception {
        stateStore.fixFileUpdateTime(time);
        stateStore.fixPartitionUpdateTime(time);
        setup.run();
    }

    private void deleteOldTransactionsAt(Instant time) {
        new TransactionLogTransactionDeleter(tableProperties, snapshots, filesLogStore, partitionsLogStore, List.of(time).iterator()::next)
                .deleteOldTransactions();
    }

    /**
     * A setup function.
     */
    public interface SetupFunction {

        /**
         * Performs the setup.
         *
         * @throws Exception if something fails
         */
        void run() throws Exception;
    }
}
