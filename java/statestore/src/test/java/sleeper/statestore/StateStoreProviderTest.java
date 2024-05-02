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
package sleeper.statestore;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.inmemory.StateStoreTestHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStoreProviderTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final Map<String, StateStore> tableIdToStateStore = new HashMap<>();
    private final List<String> tablesLoaded = new ArrayList<>();

    @Test
    void shouldCacheStateStore() {
        // Given
        TableProperties table = createStateStore("test-table-id", "test-table");

        // When
        StateStoreProvider provider = provider();
        provider.getStateStore(table);
        provider.getStateStore(table);
        provider.getStateStore(table);

        // Then
        assertThat(tablesLoaded).containsExactly("test-table-id");
    }

    private TableProperties createStateStore(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition(schema);
        tableIdToStateStore.put(tableId, stateStore);
        return tableProperties;
    }

    private StateStoreProvider provider() {
        return new StateStoreProvider(tableProperties -> {
            tablesLoaded.add(tableProperties.get(TABLE_ID));
            return tableIdToStateStore.get(tableProperties.get(TABLE_ID));
        });
    }
}
