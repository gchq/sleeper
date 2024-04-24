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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.table.InMemoryTableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
    private final TablePropertiesStore store = InMemoryTableProperties.getStore();
    private final TablePropertiesProvider provider = new TablePropertiesProvider(instanceProperties, store, Instant::now);
    private final StateStoreProvider stateStoreProvider = new StateStoreProvider(instanceProperties, s3Client, dynamoDBClient, new Configuration());

    @BeforeEach
    public void setup() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
    }

    @Test
    void shouldCreateSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore = createStateStore(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));

        // When
        runSnapshotCreator(forTableIds("test-table-id-1"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .contains(new LatestSnapshots(
                        filesSnapshot(table, "/snapshots/0-files.parquet", 0),
                        partitionsSnapshot(table, "/snapshots/0-partitions.parquet", 0)));
    }

    @Test
    void shouldCreateSnapshotsForMultipleTables() throws Exception {
        // Given
        TableProperties table1 = createTable("test-table-id-1", "test-table-1");
        StateStore stateStore1 = createStateStore(table1);
        stateStore1.initialise();
        FileReferenceFactory factory1 = FileReferenceFactory.from(stateStore1);
        stateStore1.addFile(factory1.rootFile(123L));

        TableProperties table2 = createTable("test-table-id-2", "test-table-2");
        StateStore stateStore2 = createStateStore(table2);
        stateStore2.initialise();
        FileReferenceFactory factory2 = FileReferenceFactory.from(stateStore2);
        stateStore2.addFile(factory2.rootFile(456L));

        // When
        runSnapshotCreator(forTableIds("test-table-id-1", "test-table-id-2"));

        // Then
        assertThat(snapshotStore(table1).getLatestSnapshots())
                .contains(new LatestSnapshots(
                        filesSnapshot(table1, "/snapshots/0-files.parquet", 0),
                        partitionsSnapshot(table1, "/snapshots/0-partitions.parquet", 0)));
        assertThat(snapshotStore(table2).getLatestSnapshots())
                .contains(new LatestSnapshots(
                        filesSnapshot(table2, "/snapshots/0-files.parquet", 0),
                        partitionsSnapshot(table2, "/snapshots/0-partitions.parquet", 0)));
    }

    private void runSnapshotCreator(InvokeForTableRequest tableRequest) throws StateStoreException {
        new TransactionLogSnapshotCreator(
                instanceProperties, provider, stateStoreProvider, dynamoDBClient, new Configuration())
                .run(tableRequest);
    }

    private InvokeForTableRequest forTableIds(String... tableIds) {
        return new InvokeForTableRequest(List.of(tableIds));
    }

    private DynamoDBTransactionLogSnapshotStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, table, dynamoDBClient);
    }

    private TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
        store.createTable(tableProperties);
        return tableProperties;
    }

    private StateStore createStateStore(TableProperties table) {
        return DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, table, dynamoDBClient, s3Client)
                .maxAddTransactionAttempts(1)
                .build();
    }

    private TransactionLogSnapshot filesSnapshot(TableProperties table, String path, long transactionNumber) {
        return TransactionLogSnapshot.forFiles(getFilesPath(table, transactionNumber), transactionNumber);
    }

    private TransactionLogSnapshot partitionsSnapshot(TableProperties table, String path, long transactionNumber) {
        return TransactionLogSnapshot.forPartitions(getPartitionsPath(table, transactionNumber), transactionNumber);
    }

    private String getFilesPath(TableProperties tableProperties, long transactionNumber) {
        return "file://" + instanceProperties.get(DATA_BUCKET) +
                "/" + tableProperties.get(TABLE_ID) +
                "/snapshots/" + transactionNumber + "-files.parquet";
    }

    private String getPartitionsPath(TableProperties tableProperties, long transactionNumber) {
        return "file://" + instanceProperties.get(DATA_BUCKET) +
                "/" + tableProperties.get(TABLE_ID) +
                "/snapshots/" + transactionNumber + "-partitions.parquet";
    }
}
