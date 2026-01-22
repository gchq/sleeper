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
package sleeper.core.statestore.testutils;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableStatus;

import java.util.Map;
import java.util.Objects;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

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

    /**
     * Creates a state store provider that will only load a particular table.
     *
     * @param  singleTableProperties the table properties
     * @param  stateStore            the state store
     * @return                       the provider
     */
    public static StateStoreProvider singleTable(TableProperties singleTableProperties, StateStore stateStore) {
        return StateStoreProvider.noCacheSizeLimit(tableProperties -> {
            TableStatus requestedTable = tableProperties.getStatus();
            if (!Objects.equals(requestedTable, singleTableProperties.getStatus())) {
                throw new IllegalArgumentException("Table not found: " + requestedTable);
            }
            return stateStore;
        });
    }

    /**
     * Creates a state store provider that will load the given state stores.
     *
     * @param  stateStoreByTableId a map of table ID to state store
     * @return                     the provider
     */
    public static StateStoreProvider byTableId(Map<String, StateStore> stateStoreByTableId) {
        return StateStoreProvider.noCacheSizeLimit(tableProperties -> {
            String tableId = tableProperties.get(TABLE_ID);
            if (!stateStoreByTableId.containsKey(tableId)) {
                throw new IllegalArgumentException("Table not found by ID: " + tableId);
            }
            return stateStoreByTableId.get(tableId);
        });
    }

    /**
     * Creates a state store provider that will load the given state stores.
     *
     * @param  stateStoreByTableName a map of table name to state store
     * @return                       the provider
     */
    public static StateStoreProvider byTableName(Map<String, StateStore> stateStoreByTableName) {
        return StateStoreProvider.noCacheSizeLimit(tableProperties -> {
            String tableName = tableProperties.get(TABLE_NAME);
            if (!stateStoreByTableName.containsKey(tableName)) {
                throw new IllegalArgumentException("Table not found by name: " + tableName);
            }
            return stateStoreByTableName.get(tableName);
        });
    }
}
