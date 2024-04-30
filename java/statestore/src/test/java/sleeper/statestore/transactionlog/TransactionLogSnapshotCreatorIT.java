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
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;
import sleeper.statestore.transactionlog.TransactionLogSnapshotCreator.LatestSnapshotsLoader;
import sleeper.statestore.transactionlog.TransactionLogSnapshotCreator.SnapshotSaver;

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

public class TransactionLogSnapshotCreatorIT extends TransactionLogStateStoreTestBase {
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
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithTransactions(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));

        // When
        runSnapshotCreator(table);

        // Then
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
        StateStore stateStore1 = createStateStoreWithTransactions(table1);
        stateStore1.initialise();
        FileReferenceFactory factory1 = FileReferenceFactory.from(stateStore1);
        stateStore1.addFile(factory1.rootFile(123L));

        TableProperties table2 = createTable("test-table-id-2", "test-table-2");
        StateStore stateStore2 = createStateStoreWithTransactions(table2);
        stateStore2.initialise();
        FileReferenceFactory factory2 = FileReferenceFactory.from(stateStore2);
        stateStore2.addFile(factory2.rootFile(456L));

        // When
        runSnapshotCreator(table1);
        runSnapshotCreator(table2);

        // Then
        assertThat(snapshotStore(table1).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table1, 1),
                        partitionsSnapshot(table1, 1)));
        assertThat(snapshotStore(table2).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table2, 1),
                        partitionsSnapshot(table2, 1)));
    }

    @Test
    void shouldCreateMultipleSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123L)
                .buildTree();
        FileReferenceFactory factory = FileReferenceFactory.from(tree);
        StateStore stateStore = createStateStoreWithTransactions(table);
        stateStore.initialise();
        FileReference file1 = factory.rootFile(123L);
        stateStore.addFile(file1);
        runSnapshotCreator(table);

        // When
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                tree.getPartition("root"), tree.getPartition("L"), tree.getPartition("R"));
        FileReference file2 = factory.partitionFile("L", 456L);
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
        StateStore stateStore = createStateStoreWithTransactions(table);
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
    void shouldRemoveSnapshotFilesIfDynamoTransactionFailed() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStoreWithTransactions(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));

        // When / Then
        assertThatThrownBy(() -> runSnapshotCreator(table, failedUpdate()))
                .isInstanceOf(RuntimeException.class);
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(null, null));
        assertThat(Files.exists(filesSnapshotPath(table, 1))).isFalse();
        assertThat(Files.exists(partitionsSnapshotPath(table, 1))).isFalse();
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
        new TransactionLogSnapshotCreator(
                instanceProperties, table,
                fileTransactionStoreByTableId.get(table.get(TABLE_ID)),
                partitionTransactionStoreByTableId.get(table.get(TABLE_ID)),
                configuration, latestSnapshotsLoader, snapshotSaver)
                .createSnapshot();
    }

    private StateStore createStateStoreWithTransactions(TableProperties table) {
        TransactionLogStore fileTransactionStore = new InMemoryTransactionLogStore();
        TransactionLogStore partitionTransactionStore = new InMemoryTransactionLogStore();
        fileTransactionStoreByTableId.put(table.get(TABLE_ID), fileTransactionStore);
        partitionTransactionStoreByTableId.put(table.get(TABLE_ID), partitionTransactionStore);
        return TransactionLogStateStore.builder()
                .sleeperTable(table.getStatus())
                .schema(table.getSchema())
                .filesLogStore(fileTransactionStore)
                .partitionsLogStore(partitionTransactionStore)
                .build();
    }

    private DynamoDBTransactionLogSnapshotStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, table, dynamoDBClient);
    }

    private TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
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

    private SnapshotSaver failedUpdate() {
        return snapshot -> {
            throw new DuplicateSnapshotException("test.parquet", new Exception());
        };
    }
}
