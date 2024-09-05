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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableStatus;

import java.util.Map;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Test helper to implement state store provider with fixed state stores. Replaces StateStoreFactory with
 * pre-built state store instances.
 */
public class FixedStateStoreProvider extends StateStoreProvider {
    private static final int DEFAULT_STATESTORE_CACHE_SIZE = 10;

    public FixedStateStoreProvider(TableProperties singleTableProperties, StateStore stateStore) {
        super(DEFAULT_STATESTORE_CACHE_SIZE, tableProperties -> {
            TableStatus requestedTable = tableProperties.getStatus();
            if (!Objects.equals(requestedTable, singleTableProperties.getStatus())) {
                throw new IllegalArgumentException("Table not found: " + requestedTable);
            }
            return stateStore;
        });
    }

    public FixedStateStoreProvider(Map<String, StateStore> stateStoreByTableName) {
        super(DEFAULT_STATESTORE_CACHE_SIZE, tableProperties -> {
            String tableName = tableProperties.get(TABLE_NAME);
            if (!stateStoreByTableName.containsKey(tableName)) {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }
            return stateStoreByTableName.get(tableName);
        });
    }

    public static StateStoreProvider byTableId(Map<String, StateStore> stateStoreByTableId) {
        return new StateStoreProvider(DEFAULT_STATESTORE_CACHE_SIZE, tableProperties -> {
            String tableId = tableProperties.get(TABLE_ID);
            if (!stateStoreByTableId.containsKey(tableId)) {
                throw new IllegalArgumentException("Table not found by ID: " + tableId);
            }
            return stateStoreByTableId.get(tableId);
        });
    }
}
