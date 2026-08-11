/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class AddTableTest {
    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = createSchemaWithKey("key", new StringType());
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore();
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_NAME, "add-table");
    }

    @Test
    void shouldAddTable() {
        // Given
        AddTable addTable = new AddTable(tablePropertiesStore, stateStoreProvider);

        // When
        addTable.addTable(tableProperties, List.of());

        // Then
        assertThat(tablePropertiesStore.streamAllTableStatuses())
                .flatExtracting(TableStatus::getTableName).containsExactly("add-table");
        assertThat(stateStoreProvider.getStateStore(tableProperties).getAllPartitions())
                .isEqualTo(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
    }

    @Test
    void shouldNotAddTableWhenPropertiesAreInvalid() {
        // Given
        tableProperties.set(TABLE_ONLINE, "invalid-value");
        AddTable addTable = new AddTable(tablePropertiesStore, stateStoreProvider);

        // When / then
        assertThatThrownBy(() -> addTable.addTable(tableProperties, List.of()))
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }
}
