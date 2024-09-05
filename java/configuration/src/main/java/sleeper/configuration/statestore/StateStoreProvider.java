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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import static sleeper.configuration.properties.instance.CommonProperty.STATESTORE_PROVIDER_CACHE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

/**
 * Caches Sleeper table state store objects up to a maximum size. If the cache is full, the oldest state store objects
 * be removed from the cache. An instance of this class cannot be used concurrently in multiple threads,
 * as the cache is not thread-safe.
 */
public class StateStoreProvider {
    private final int cacheSize;
    private final Factory stateStoreFactory;
    private final Map<String, StateStore> tableIdToStateStoreCache = new HashMap<>();
    private final Queue<String> tableIds = new LinkedList<>();

    public StateStoreProvider(InstanceProperties instanceProperties, Factory stateStoreFactory) {
        this(instanceProperties.getInt(STATESTORE_PROVIDER_CACHE_SIZE), stateStoreFactory);
    }

    protected StateStoreProvider(int cacheSize, Factory stateStoreFactory) {
        this.cacheSize = cacheSize;
        this.stateStoreFactory = stateStoreFactory;
    }

    /**
     * Retrieves or creates the state store client for the given Sleeper table.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the state store
     */
    public StateStore getStateStore(TableProperties tableProperties) {
        String tableId = tableProperties.get(TABLE_ID);
        if (!tableIdToStateStoreCache.containsKey(tableId)) {
            if (tableIdToStateStoreCache.size() == cacheSize) {
                tableIdToStateStoreCache.remove(tableIds.poll());
            }
            StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
            tableIdToStateStoreCache.put(tableId, stateStore);
            tableIds.add(tableId);
        }
        return tableIdToStateStoreCache.get(tableId);
    }

    /**
     * Creates an instance of the state store client for a Sleeper table. Implemented by {@link StateStoreFactory}.
     */
    public interface Factory {

        /**
         * Creates a state store client.
         *
         * @param  tableProperties the Sleeper table properties
         * @return                 the state store
         */
        StateStore getStateStore(TableProperties tableProperties);
    }
}
