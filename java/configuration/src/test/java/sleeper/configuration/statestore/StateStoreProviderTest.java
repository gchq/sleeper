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
package sleeper.configuration.statestore;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.StateStoreTestHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.STATESTORE_PROVIDER_CACHE_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStoreProviderTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final Map<String, StateStore> tableIdToStateStore = new HashMap<>();
    private final List<String> tablesLoaded = new ArrayList<>();

    @Test
    void shouldCacheStateStore() {
        // Given
        TableProperties table = createTable("test-table-id", "test-table");
        StateStore store = createStateStore(table);

        // When
        StateStoreProvider provider = provider();
        StateStore retrievedStore1 = provider.getStateStore(table);
        StateStore retrievedStore2 = provider.getStateStore(table);
        StateStore retrievedStore3 = provider.getStateStore(table);

        // Then
        assertThat(retrievedStore1).isEqualTo(store);
        assertThat(retrievedStore2).isEqualTo(store);
        assertThat(retrievedStore3).isEqualTo(store);
        assertThat(tablesLoaded).containsExactly("test-table-id");
    }

    @Test
    void shouldRemoveOldestCacheEntryWhenLimitHasBeenReached() {
        // Given
        instanceProperties.setNumber(STATESTORE_PROVIDER_CACHE_SIZE, 2);
        TableProperties table1 = createTable("table-id-1", "test-table-1");
        StateStore store1 = createStateStore(table1);
        TableProperties table2 = createTable("table-id-2", "test-table-2");
        StateStore store2 = createStateStore(table2);
        TableProperties table3 = createTable("table-id-3", "test-table-3");
        StateStore store3 = createStateStore(table3);

        // When
        StateStoreProvider provider = provider();
        StateStore retrievedStore1 = provider.getStateStore(table1);
        StateStore retrievedStore2 = provider.getStateStore(table2);
        // Cache size has been reached, oldest state store will be removed from cache
        StateStore retrievedStore3 = provider.getStateStore(table3);
        StateStore retrievedStore4 = provider.getStateStore(table1);

        // Then
        assertThat(retrievedStore1).isEqualTo(store1);
        assertThat(retrievedStore2).isEqualTo(store2);
        assertThat(retrievedStore3).isEqualTo(store3);
        assertThat(retrievedStore4).isEqualTo(store1);
        assertThat(tablesLoaded).containsExactly(
                "table-id-1",
                "table-id-2",
                "table-id-3",
                "table-id-1");
    }

    private TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        return tableProperties;
    }

    private StateStore createStateStore(TableProperties table) {
        StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition(schema);
        tableIdToStateStore.put(table.get(TABLE_ID), stateStore);
        return stateStore;
    }

    private StateStoreProvider provider() {
        return new StateStoreProvider(instanceProperties, tableProperties -> {
            tablesLoaded.add(tableProperties.get(TABLE_ID));
            return tableIdToStateStore.get(tableProperties.get(TABLE_ID));
        });
    }
}
