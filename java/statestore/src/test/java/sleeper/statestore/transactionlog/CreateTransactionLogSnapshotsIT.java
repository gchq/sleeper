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

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.statestore.transactionlog.CreateTransactionLogSnapshots.LatestSnapshotsLoader;
import sleeper.statestore.transactionlog.CreateTransactionLogSnapshots.SnapshotSaver;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class CreateTransactionLogSnapshotsIT extends TransactionLogStateStoreTestBase {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new LongType());
    private final Map<String, TransactionLogStore> partitionTransactionStoreByTableId = new HashMap<>();
    private final Map<String, TransactionLogStore> fileTransactionStoreByTableId = new HashMap<>();

    @BeforeEach
    public void setup() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
    }

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
        inMemoryStateStore.initialise(partitions.getAllPartitions());
        inMemoryStateStore.addFile(file);

        // When we create a snapshot from the in-memory transactions
        runSnapshotCreator(table);

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
        inMemoryStateStore1.initialise(partitions1.getAllPartitions());
        inMemoryStateStore1.addFile(file1);

        TableProperties table2 = createTable("test-table-id-2", "test-table-2");
        PartitionTree partitions2 = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "C", "D", 123L)
                .buildTree();
        FileReference file2 = FileReferenceFactory.fromUpdatedAt(partitions2, DEFAULT_UPDATE_TIME)
                .rootFile("file2.parquet", 123L);
        StateStore inMemoryStateStore2 = createStateStoreWithInMemoryTransactionLog(table2);
        inMemoryStateStore2.initialise(partitions2.getAllPartitions());
        inMemoryStateStore2.addFile(file2);

        // When
        runSnapshotCreator(table1);
        runSnapshotCreator(table2);

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
    }

    @Test
    void shouldCreateMultipleSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        FileReference file1 = FileReferenceFactory.from(partitions.buildTree())
                .rootFile(123L);
        stateStore.addFile(file1);
        runSnapshotCreator(table);

        // When
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(stateStore, "root");
        FileReference file2 = FileReferenceFactory.from(partitions.buildTree())
                .partitionFile("L", 456L);
        stateStore.addFile(file2);
        runSnapshotCreator(table);

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
    }

    @Test
    void shouldSkipCreatingSnapshotsIfStateHasNotUpdatedSinceLastSnapshot() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));
        runSnapshotCreator(table);

        // When
        runSnapshotCreator(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
    }

    @Test
    void shouldNotCreateSnapshotForTableWithNoTransactions() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");

        // When
        runSnapshotCreator(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(LatestSnapshots.empty());
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore(table).getPartitionsSnapshots()).isEmpty();
    }

    @Test
    void shouldNotCreateFileSnapshotForTableWithOnlyPartitionTransactions() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();

        // When
        runSnapshotCreator(table);

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(null, partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(partitionsSnapshot(table, 1));
    }

    @Test
    void shouldRemoveFilesSnapshotFileIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        runSnapshotCreator(table);
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> runSnapshotCreator(table, failedUpdate(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getFilesSnapshots()).isEmpty();
        assertThat(Files.exists(filesSnapshotPath(table, 1))).isFalse();
    }

    @Test
    void shouldRemovePartitionsSnapshotFileIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();

        // When / Then
        IllegalStateException exception = new IllegalStateException();
        assertThatThrownBy(() -> runSnapshotCreator(table, failedUpdate(exception)))
                .isSameAs(exception);
        assertThat(snapshotStore(table).getPartitionsSnapshots()).isEmpty();
        assertThat(Files.exists(partitionsSnapshotPath(table, 1))).isFalse();
    }

    @Test
    void shouldNotCreateSnapshotIfLoadingPreviousPartitionSnapshotFails() throws Exception {
        // Given we delete the partitions file for the last snapshot
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise(partitions.buildList());
        runSnapshotCreator(table);
        TransactionLogSnapshot snapshot = getLatestPartitionsSnapshot(table);
        deleteSnapshotFile(snapshot);
        // And we add a transaction that would trigger a new snapshot
        partitions.splitToNewChildren("root", "L", "R", 123L)
                .applySplit(stateStore, "root");

        // When / Then
        assertThatThrownBy(() -> runSnapshotCreator(table))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(FileNotFoundException.class);
        assertThat(snapshotStore(table).getPartitionsSnapshots()).containsExactly(snapshot);
    }

    @Test
    void shouldNotCreateSnapshotIfLoadingPreviousFileSnapshotFails() throws Exception {
        // Given we delete the files file for the last snapshot
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        stateStore.addFile(FileReferenceFactory.from(stateStore).rootFile("file1.parquet", 123));
        runSnapshotCreator(table);
        TransactionLogSnapshot snapshot = getLatestFilesSnapshot(table);
        deleteSnapshotFile(snapshot);
        // And we add a transaction that would trigger a new snapshot
        stateStore.addFile(FileReferenceFactory.from(stateStore).rootFile("file2.parquet", 456));

        // When / Then
        assertThatThrownBy(() -> runSnapshotCreator(table))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(FileNotFoundException.class);
        assertThat(snapshotStore(table).getFilesSnapshots()).containsExactly(snapshot);
    }

    private TransactionLogSnapshot getLatestPartitionsSnapshot(TableProperties table) {
        return snapshotStore(table).getLatestSnapshots().getPartitionsSnapshot().orElseThrow();
    }

    private TransactionLogSnapshot getLatestFilesSnapshot(TableProperties table) {
        return snapshotStore(table).getLatestSnapshots().getFilesSnapshot().orElseThrow();
    }

    private void deleteSnapshotFile(TransactionLogSnapshot snapshot) throws Exception {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(snapshot.getPath());
        FileSystem fs = path.getFileSystem(configuration);
        fs.delete(path, false);
    }

    private void runSnapshotCreator(TableProperties table) {
        DynamoDBTransactionLogSnapshotStore snapshotStore = snapshotStore(table);
        runSnapshotCreator(table, snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    private void runSnapshotCreator(
            TableProperties table, SnapshotSaver snapshotSaver) {
        DynamoDBTransactionLogSnapshotStore snapshotStore = snapshotStore(table);
        runSnapshotCreator(table, snapshotStore::getLatestSnapshots, snapshotSaver);
    }

    private void runSnapshotCreator(
            TableProperties table, LatestSnapshotsLoader latestSnapshotsLoader, SnapshotSaver snapshotSaver) {
        new CreateTransactionLogSnapshots(
                instanceProperties, table,
                fileTransactionStoreByTableId.get(table.get(TABLE_ID)),
                partitionTransactionStoreByTableId.get(table.get(TABLE_ID)),
                configuration, latestSnapshotsLoader, snapshotSaver)
                .createSnapshot();
    }

    private StateStore createStateStoreWithInMemoryTransactionLog(TableProperties table) {
        StateStore stateStore = TransactionLogStateStore.builder()
                .sleeperTable(table.getStatus())
                .schema(table.getSchema())
                .filesLogStore(fileTransactionStoreByTableId.get(table.get(TABLE_ID)))
                .partitionsLogStore(partitionTransactionStoreByTableId.get(table.get(TABLE_ID)))
                .build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    private DynamoDBTransactionLogSnapshotStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, table, dynamoDBClient);
    }

    private TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
        fileTransactionStoreByTableId.put(tableId, new InMemoryTransactionLogStore());
        partitionTransactionStoreByTableId.put(tableId, new InMemoryTransactionLogStore());
        return tableProperties;
    }

    private TransactionLogSnapshot filesSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshot.forFiles(getBasePath(instanceProperties, table), transactionNumber);
    }

    private TransactionLogSnapshot partitionsSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshot.forPartitions(getBasePath(instanceProperties, table), transactionNumber);
    }

    private Path filesSnapshotPath(TableProperties table, long transactionNumber) {
        return Path.of(TransactionLogSnapshot.forFiles(getBasePathNoFs(instanceProperties, table), 1).getPath());
    }

    private Path partitionsSnapshotPath(TableProperties table, long transactionNumber) {
        return Path.of(TransactionLogSnapshot.forPartitions(getBasePathNoFs(instanceProperties, table), 1).getPath());
    }

    private static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    private static String getBasePathNoFs(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    private SnapshotSaver failedUpdate(RuntimeException exception) {
        return snapshot -> {
            throw exception;
        };
    }
}
