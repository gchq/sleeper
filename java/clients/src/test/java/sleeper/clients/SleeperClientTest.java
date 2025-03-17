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
package sleeper.clients;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.core.recordretrieval.InMemoryDataStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class SleeperClientTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance(tableIndex);
    Schema schema = schemaWithKey("key", new StringType());
    InMemoryDataStore dataStore = new InMemoryDataStore();
    SleeperClient sleeperClient = SleeperClient.builder()
            .instanceProperties(instanceProperties)
            .tableIndex(tableIndex)
            .tablePropertiesStore(tablePropertiesStore)
            .tablePropertiesProvider(new TablePropertiesProvider(instanceProperties, tablePropertiesStore))
            .stateStoreProvider(InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable()))
            .recordRetrieverProvider(dataStore)
            .build();

    @Test
    void shouldAddTable() {
        // Given
        TableProperties tableProperties = createTableProperties("test-table");
        List<Object> splitPoints = List.of();

        // When
        sleeperClient.addTable(tableProperties, splitPoints);

        // Then
        assertThat(sleeperClient.streamAllTables()).containsExactly(tableProperties.getStatus());
        assertThat(sleeperClient.getStateStore("test-table").getAllPartitions()).isEqualTo(
                new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
    }

    @Test
    void shouldNotAddTableWithInvalidProperties() {
        // Given
        TableProperties tableProperties = createTableProperties("test-table");
        List<Object> splitPoints = List.of();
        tableProperties.set(TABLE_ONLINE, "not-a-boolean");

        // When / Then
        assertThatThrownBy(() -> sleeperClient.addTable(tableProperties, splitPoints))
                .isInstanceOf(SleeperPropertiesInvalidException.class);
        assertThat(sleeperClient.streamAllTables()).isEmpty();
    }

    private TableProperties createTableProperties(String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

}
