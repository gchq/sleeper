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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBTransactionLogSnapshotStoreIT extends TransactionLogStateStoreTestBase {
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private TransactionLogSnapshotStore snapshotStore = snapshotStore();

    @BeforeEach
    void setup() {
        new DynamoDBTransactionLogSnapshotStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    @Test
    void shouldStoreFilesSnapshot() throws Exception {
        // Given / When
        snapshotStore.saveFiles("snapshot/1-files.parquet", 1);

        // Then
        assertThat(snapshotStore.getFilesSnapshots())
                .containsExactly(TransactionLogSnapshot.forFiles("snapshot/1-files.parquet", 1));
        assertThat(snapshotStore.getPartitionsSnapshots()).isEmpty();
    }

    @Test
    void shouldStorePartitionsSnapshot() throws Exception {
        // Given / When
        snapshotStore.savePartitions("snapshot/1-partitions.parquet", 1);

        // Then
        assertThat(snapshotStore.getFilesSnapshots()).isEmpty();
        assertThat(snapshotStore.getPartitionsSnapshots())
                .containsExactly(TransactionLogSnapshot.forPartitions("snapshot/1-partitions.parquet", 1));
    }

    private TransactionLogSnapshotStore snapshotStore() {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDBClient, Instant::now);
    }
}
