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
package sleeper.core.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.JvmMemoryUse;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_PROVIDER_CACHE_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Caches Sleeper table state store objects up to a maximum size. If the cache is full, the oldest state store objects
 * be removed from the cache. An instance of this class cannot be used concurrently in multiple threads,
 * as the cache is not thread-safe.
 */
public class StateStoreProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateStoreProvider.class);

    private final Factory stateStoreFactory;
    private final JvmMemoryUse.Provider memoryProvider;
    private final Map<String, StateStore> tableIdToStateStoreCache = new HashMap<>();
    private final Queue<String> tableIds = new LinkedList<>();
    private final int cacheSize;
    private final long memoryBytesToKeepFree;

    public StateStoreProvider(InstanceProperties instanceProperties, Factory stateStoreFactory) {
        this(instanceProperties.getInt(STATESTORE_PROVIDER_CACHE_SIZE), stateStoreFactory);
    }

    public StateStoreProvider(int cacheSize, Factory stateStoreFactory) {
        this(stateStoreFactory, JvmMemoryUse.getProvider(), cacheSize, -1);
    }

    public StateStoreProvider(Factory stateStoreFactory, JvmMemoryUse.Provider memoryProvider, int cacheSize, long memoryBytesToKeepFree) {
        this.stateStoreFactory = stateStoreFactory;
        this.memoryProvider = memoryProvider;
        this.cacheSize = cacheSize;
        this.memoryBytesToKeepFree = memoryBytesToKeepFree;
    }

    /**
     * Creates a state store provider with no limit to the number of tables that can be cached. The cache must be
     * managed explicitly by calling methods on the provider.
     *
     * @param  stateStoreFactory the factory to create state store objects
     * @return                   the provider
     */
    public static StateStoreProvider noCacheSizeLimit(Factory stateStoreFactory) {
        return new StateStoreProvider(stateStoreFactory, () -> new JvmMemoryUse(0, 0, 0), -1, -1);
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
     * Checks the available head space and removes tables from the cache if necessary. Allows setting tables that should
     * not be removed.
     *
     * @param requiredTableIds the set of IDs of tables that should not be removed from the cache
     */
    public void ensureEnoughHeapSpaceAvailable(Set<String> requiredTableIds) {
        JvmMemoryUse memory = memoryProvider.getMemory();
        LOGGER.debug("Keeping {} free. Found memory use: {}", memoryBytesToKeepFree, memory);
    }

    /**
     * Remove a specific table's state store from the cache.
     *
     * @param  tableId the Sleeper table ID
     * @return         true if the state store for the requested Sleeper table was in the cache and has been removed
     */
    public boolean removeStateStoreFromCache(String tableId) {
        return tableIdToStateStoreCache.remove(tableId) != null
                && tableIds.remove(tableId);
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
