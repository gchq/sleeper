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
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.nio.file.Files;
import java.nio.file.Path;
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

public class DeleteTransactionLogSnapshotsIT extends TransactionLogStateStoreTestBase {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new LongType());

    @BeforeEach
    public void setup() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
    }

    @Test
    void shouldDeleteOldFilesSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStore(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotAt(table, Instant.parse("2024-04-25T11:24:00Z"));
        stateStore.addFile(factory.rootFile("test2.parquet", 456L));
        createSnapshotAt(table, Instant.parse("2024-04-26T15:24:00Z"));
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // When
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(
                        filesSnapshot(table, 2),
                        filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 1));
        assertThat(Files.exists(Path.of(filesSnapshot(table, 1).getPath()))).isFalse();
        assertThat(Files.exists(Path.of(partitionsSnapshot(table, 1).getPath()))).isFalse();
    }

    private void createSnapshotAt(TableProperties table, Instant creationTime) throws Exception {
        CreateTransactionLogSnapshots.from(instanceProperties, table, s3Client, dynamoDBClient, configuration, () -> creationTime)
                .createSnapshot();
    }

    private void deleteSnapshotsAt(TableProperties table, Instant deletionTime) {
        new DeleteTransactionLogSnapshots(
                instanceProperties, table, dynamoDBClient, configuration, () -> deletionTime)
                .deleteSnapshots();
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

    private static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    public interface SetupStateStore {
        void run(StateStore stateStore) throws StateStoreException;
    }
}
