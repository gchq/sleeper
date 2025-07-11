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
package sleeper.statestore.transactionlog.snapshots;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator.LatestSnapshotsMetadataLoader;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotSaver.SnapshotMetadataSaver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TransactionLogSnapshotCreatorIT extends TransactionLogSnapshotTestBase {

    @Test
    void shouldCreateSnapshotsForOneTable() throws Exception {
        // Given we create a transaction log in memory
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123L)
                .buildTree();
        FileReference file = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_UPDATE_TIME).rootFile(123);
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore inMemoryStateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(inMemoryStateStore).initialise(partitions.getAllPartitions());
        update(inMemoryStateStore).addFile(file);

        // When we create a snapshot from the in-memory transactions
        createSnapshots(table);

        // Then when we read from a state store with no transaction log, we load the state from the snapshot
        StateStore stateStore = createStateStore(table);
        assertThat(stateStore.getAllPartitions()).isEqualTo(partitions.getAllPartitions());
        assertThat(stateStore.getFileReferences()).containsExactly(file);
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 1));
        assertThat(filesInDataBucket())
                .containsExactlyInAnyOrder(
                        filesSnapshotObjectKey(table, 1),
                        partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldCreateSnapshotsForMultipleTables() throws Exception {
        // Given
        TableProperties table1 = createTable("test-table-id-1", "test-table-1");
        PartitionTree partitions1 = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "A", "B", 123L)
                .buildTree();
        FileReference file1 = FileReferenceFactory.fromUpdatedAt(partitions1, DEFAULT_UPDATE_TIME)
                .rootFile("file1.parquet", 123L);
        StateStore inMemoryStateStore1 = createStateStoreWithInMemoryTransactionLog(table1);
        update(inMemoryStateStore1).initialise(partitions1.getAllPartitions());
        update(inMemoryStateStore1).addFile(file1);

        TableProperties table2 = createTable("test-table-id-2", "test-table-2");
        PartitionTree partitions2 = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "C", "D", 123L)
                .buildTree();
        FileReference file2 = FileReferenceFactory.fromUpdatedAt(partitions2, DEFAULT_UPDATE_TIME)
                .rootFile("file2.parquet", 123L);
        StateStore inMemoryStateStore2 = createStateStoreWithInMemoryTransactionLog(table2);
        update(inMemoryStateStore2).initialise(partitions2.getAllPartitions());
        update(inMemoryStateStore2).addFile(file2);

        // When
        createSnapshots(table1);
        createSnapshots(table2);

        // Then
        StateStore stateStore1 = createStateStore(table1);
        assertThat(stateStore1.getAllPartitions()).isEqualTo(partitions1.getAllPartitions());
        assertThat(stateStore1.getFileReferences()).containsExactly(file1);
        assertThat(snapshotStore(table1).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table1, 1),
                        partitionsSnapshot(table1, 1)));
        assertThat(snapshotStore(table1).getFilesSnapshots())
                .containsExactly(filesSnapshot(table1, 1));
        assertThat(snapshotStore(table1).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table1, 1));

        StateStore stateStore2 = createStateStore(table2);
        assertThat(stateStore2.getAllPartitions()).isEqualTo(partitions2.getAllPartitions());
        assertThat(stateStore2.getFileReferences()).containsExactly(file2);
        assertThat(snapshotStore(table2).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table2, 1),
                        partitionsSnapshot(table2, 1)));
        assertThat(snapshotStore(table2).getFilesSnapshots())
                .containsExactly(filesSnapshot(table2, 1));
        assertThat(snapshotStore(table2).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table2, 1));
        assertThat(filesInDataBucket())
                .containsExactlyInAnyOrder(
                        filesSnapshotObjectKey(table1, 1),
                        partitionsSnapshotObjectKey(table1, 1),
                        filesSnapshotObjectKey(table2, 1),
                        partitionsSnapshotObjectKey(table2, 1));
    }

    @Test
    void shouldCreateMultipleSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(partitions.buildList());
        FileReference file1 = FileReferenceFactory.from(partitions.buildTree())
                .rootFile(123L);
        update(stateStore).addFile(file1);
        createSnapshots(table);

        // When
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(stateStore, "root");
        FileReference file2 = FileReferenceFactory.from(partitions.buildTree())
                .partitionFile("L", 456L);
        update(stateStore).addFile(file2);
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 2),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(
                        filesSnapshot(table, 1),
                        filesSnapshot(table, 2));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(
                        partitionsSnapshot(table, 1),
                        partitionsSnapshot(table, 2));
        assertThat(filesInDataBucket())
                .containsExactlyInAnyOrder(
                        filesSnapshotObjectKey(table, 2),
                        filesSnapshotObjectKey(table, 1),
                        partitionsSnapshotObjectKey(table, 2),
                        partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldSkipCreatingSnapshotsIfStateHasNotUpdatedSinceLastSnapshot() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(schema);
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        update(stateStore).addFile(factory.rootFile(123L));
        createSnapshots(table);

        // When
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
        assertThat(filesInDataBucket())
                .containsExactlyInAnyOrder(
                        filesSnapshotObjectKey(table, 1),
                        partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldNotCreateSnapshotForTableWithNoTransactions() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");

        // When
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(LatestSnapshots.empty());
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore(table).getPartitionsSnapshots()).isEmpty();
        assertThat(filesInDataBucket()).isEmpty();
    }

    @Test
    void shouldNotCreateFileSnapshotForTableWithOnlyPartitionTransactions() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(schema);

        // When
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(null, partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(partitionsSnapshot(table, 1));
        assertThat(filesInDataBucket()).containsExactly(partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldRemoveFilesSnapshotFileIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(schema);
        createSnapshots(table);
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        update(stateStore).addFile(factory.rootFile(123L));

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> createSnapshots(table, failedUpdate(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(filesSnapshotFileExists(table, 1)).isFalse();
        assertThat(filesInDataBucket()).containsExactly(partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldRemovePartitionsSnapshotFileIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(schema);

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> createSnapshots(table, failedUpdate(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getPartitionsSnapshots()).isEmpty();
        assertThat(filesInDataBucket()).isEmpty();
    }

    @Test
    void shouldNotCreateSnapshotIfLoadingPreviousPartitionSnapshotFails() throws Exception {
        // Given we create a snapshot
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(partitions.buildList());
        createSnapshots(table);
        // And we add a transaction that would trigger a new snapshot creation
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(stateStore, "root");

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> createSnapshots(table, failedLoad(exception)))
                .isInstanceOf(RuntimeException.class);
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(partitionsSnapshot(table, 1));
        assertThat(filesInDataBucket()).containsExactly(partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldNotCreateSnapshotIfLoadingPreviousFileSnapshotFails() throws Exception {
        // Given we create a snapshot
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        update(stateStore).initialise(schema);
        update(stateStore).addFile(FileReferenceFactory.from(stateStore).rootFile("file1.parquet", 123));
        createSnapshots(table);
        // And we add a transaction that would trigger a new snapshot creation
        update(stateStore).addFile(FileReferenceFactory.from(stateStore).rootFile("file2.parquet", 456));

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> createSnapshots(table, failedLoad(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getFilesSnapshots()).containsExactly(filesSnapshot(table, 1));
        assertThat(filesInDataBucket())
                .containsExactlyInAnyOrder(
                        filesSnapshotObjectKey(table, 1),
                        partitionsSnapshotObjectKey(table, 1));
    }

    @Test
    void shouldCreateSnapshotFromTransactionInBodyStore() throws Exception {
        // Given we create a transaction with its body held in a store
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        PartitionTransaction transaction = new InitialisePartitionsTransaction(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .buildList());
        String key = TransactionBodyStore.createObjectKey(table);
        transactionBodyStore().store(key, table.get(TABLE_ID), transaction);
        stateStore.addTransaction(AddTransactionRequest.withTransaction(transaction).bodyKey(key).build());

        // When we create a snapshot
        createSnapshots(table);

        // Then
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(partitionsSnapshot(table, 1));
        assertThat(filesInDataBucket())
                .containsExactlyInAnyOrder(
                        partitionsSnapshotObjectKey(table, 1));
    }

    private StateStore createStateStore(TableProperties tableProperties) {
        StateStore stateStore = DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties, dynamoClient, s3Client).build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    private LatestSnapshotsMetadataLoader failedLoad(RuntimeException exception) {
        return () -> {
            throw exception;
        };
    }

    private SnapshotMetadataSaver failedUpdate(RuntimeException exception) {
        return snapshot -> {
            throw exception;
        };
    }
}
