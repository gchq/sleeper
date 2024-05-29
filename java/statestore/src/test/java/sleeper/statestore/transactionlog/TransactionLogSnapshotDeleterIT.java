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
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;

import java.io.IOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogSnapshotDeleterIT extends TransactionLogStateStoreTestBase {
    @TempDir
    private java.nio.file.Path tempDir;
    private final Schema schema = schemaWithKey("key", new LongType());
    private FileSystem fs;

    @BeforeEach
    public void setup() throws IOException {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
        fs = FileSystem.get(configuration);
    }

    @Test
    void shouldDeleteOldSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStore(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));

        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // When
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 2));
        assertThat(filesSnapshotFileExists(table, 3)).isTrue();
        assertThat(filesSnapshotFileExists(table, 1)).isFalse();
        assertThat(partitionsSnapshotFileExists(table, 2)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 1)).isFalse();
    }

    @Test
    void shouldNotDeleteOldSnapshotsIfTheyAreNotOldEnoughForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStore(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-27T11:23:00Z"));

        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // When
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:25:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactlyInAnyOrder(
                        filesSnapshot(table, 3),
                        filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactlyInAnyOrder(
                        partitionsSnapshot(table, 2),
                        partitionsSnapshot(table, 1));
        assertThat(filesSnapshotFileExists(table, 3)).isTrue();
        assertThat(filesSnapshotFileExists(table, 1)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 2)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 1)).isTrue();
    }

    @Test
    void shouldNotDeleteOldSnapshotsIfTheyAreAlsoLatestSnapshots() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStore(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));

        // When
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 1));
        assertThat(filesSnapshotFileExists(table, 1)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 1)).isTrue();
    }

    @Test
    void shouldDeleteOldSnapshotsIfSnapshotFilesHaveAlreadyBeenDeleted() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStore(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-24T11:24:00Z"));
        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));
        // Delete files for snapshots that are eligible for deletion
        deleteFilesSnapshotFile(table, 1);
        deletePartitionsSnapshotFile(table, 1);

        // When
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 2));
    }

    private void createSnapshotsAt(TableProperties table, Instant creationTime) throws Exception {
        DynamoDBTransactionLogSnapshotCreator.from(
                instanceProperties, table, s3Client, dynamoDBClient, configuration, () -> creationTime)
                .createSnapshot();
    }

    private void deleteSnapshotsAt(TableProperties table, Instant deletionTime) {
        new TransactionLogSnapshotDeleter(
                instanceProperties, table, dynamoDBClient, configuration)
                .deleteSnapshots(deletionTime);
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, table, dynamoDBClient);
    }

    private TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
        return tableProperties;
    }

    private TransactionLogSnapshotMetadata filesSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(getBasePath(instanceProperties, table), transactionNumber);
    }

    private TransactionLogSnapshotMetadata partitionsSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forPartitions(getBasePath(instanceProperties, table), transactionNumber);
    }

    private boolean filesSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        return fs.exists(new Path(filesSnapshot(table, transactionNumber).getPath()));
    }

    private boolean partitionsSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        return fs.exists(new Path(partitionsSnapshot(table, transactionNumber).getPath()));
    }

    private static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    private void deleteFilesSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        fs.delete(new Path(filesSnapshot(table, transactionNumber).getPath()), false);
    }

    private void deletePartitionsSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        fs.delete(new org.apache.hadoop.fs.Path(partitionsSnapshot(table, transactionNumber).getPath()), false);
    }
}
