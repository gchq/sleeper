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

import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogSnapshotCreatorIT extends TransactionLogStateStoreTestBase {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new LongType());
    private StateStore stateStore;

    @BeforeEach
    public void setup() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
    }

    @Test
    void shouldCreateSnapshotsWhenNoSnapshotsExist() throws Exception {
        // Given
        TableProperties table = createTable("test-table-1");
        stateStore = createStateStore(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile(123L));

        // When
        snapshotCreator(table, stateStore).run(forTables("test-table-1"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .contains(new LatestSnapshots(
                        filesSnapshot(table, "/snapshots/0-files.parquet", 0),
                        partitionsSnapshot(table, "/snapshots/0-partitions.parquet", 0)));
    }

    private TransactionLogSnapshotCreator snapshotCreator(TableProperties table, StateStore stateStore) throws Exception {
        return new TransactionLogSnapshotCreator(
                instanceProperties,
                new FixedTablePropertiesProvider(List.of(table)),
                new FixedStateStoreProvider(table, stateStore),
                dynamoDBClient, new Configuration());
    }

    private InvokeForTableRequest forTables(String... tableIds) {
        return new InvokeForTableRequest(List.of(tableIds));
    }

    private DynamoDBTransactionLogSnapshotStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, table, dynamoDBClient);
    }

    private TableProperties createTable(String tableId) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        return tableProperties;
    }

    private StateStore createStateStore(TableProperties table) {
        return DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, table, dynamoDBClient, s3Client)
                .maxAddTransactionAttempts(1)
                .build();
    }

    private TransactionLogSnapshot filesSnapshot(TableProperties table, String path, long transactionNumber) {
        return TransactionLogSnapshot.forFiles(basePathForTable(table) + path, transactionNumber);
    }

    private TransactionLogSnapshot partitionsSnapshot(TableProperties table, String path, long transactionNumber) {
        return TransactionLogSnapshot.forPartitions(basePathForTable(table) + path, transactionNumber);
    }

    private String basePathForTable(TableProperties tableProperties) {
        return "file://" + tempDir.resolve("test-table-1").toString();
    }
}
