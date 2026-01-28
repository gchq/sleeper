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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.JvmMemoryUse;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_PROVIDER_CACHE_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT;
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
        this(instanceProperties, stateStoreFactory, JvmMemoryUse.getProvider());
    }

    public StateStoreProvider(InstanceProperties instanceProperties, Factory stateStoreFactory, JvmMemoryUse.Provider memoryProvider) {
        this(stateStoreFactory, memoryProvider, instanceProperties.getInt(STATESTORE_PROVIDER_CACHE_SIZE), instanceProperties.getBytes(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT));
    }

    public StateStoreProvider(Factory stateStoreFactory, JvmMemoryUse.Provider memoryProvider, int cacheSize, long memoryBytesToKeepFree) {
        this.stateStoreFactory = stateStoreFactory;
        this.memoryProvider = memoryProvider;
        this.cacheSize = cacheSize;
        this.memoryBytesToKeepFree = memoryBytesToKeepFree;
    }

    /**
     * Creates a state store provider with no limit to the number of tables that can be cached. The cache must be
     * managed explicitly by calling methods on the provider. Note that {@link #ensureEnoughHeapSpaceAvailable()} will
     * not have a target amount of space, so will never free any space.
     *
     * @param  stateStoreFactory the factory to create state store objects
     * @return                   the provider
     */
    public static StateStoreProvider noCacheSizeLimit(Factory stateStoreFactory) {
        return new StateStoreProvider(stateStoreFactory, JvmMemoryUse.getProvider(), -1, -1);
    }

    /**
     * Creates a state store provider that only removes cached tables on calls to ensure enough heap space is available.
     * This must be triggered explicitly by calling {@link #ensureEnoughHeapSpaceAvailable()}.
     *
     * @param  instanceProperties the instance properties to configure the minimum heap space
     * @param  stateStoreFactory  the factory to create state store objects
     * @return                    the provider
     */
    public static StateStoreProvider memoryLimitOnly(InstanceProperties instanceProperties, Factory stateStoreFactory) {
        return new StateStoreProvider(stateStoreFactory, JvmMemoryUse.getProvider(), -1, instanceProperties.getBytes(STATESTORE_PROVIDER_MIN_FREE_HEAP_TARGET_AMOUNT));
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
                removeLeastRecentlyUsedStateStoreFromCache(List.of());
            }
            StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
            tableIdToStateStoreCache.put(tableId, stateStore);
        }
        tableIds.remove(tableId);
        tableIds.add(tableId);
        return tableIdToStateStoreCache.get(tableId);
    }

    /**
     * Checks the available head space and removes tables from the cache if necessary. Allows setting tables that should
     * not be removed.
     *
     * @param  requiredTableIds the set of IDs of tables that should not be removed from the cache
     * @return                  true if there is enough memory available, or we could free enough, false otherwise
     */
    public boolean ensureEnoughHeapSpaceAvailable(Collection<String> requiredTableIds) {
        JvmMemoryUse memory = memoryProvider.getMemory();
        String displayBytesToKeepFree = FileUtils.byteCountToDisplaySize(memoryBytesToKeepFree);
        if (memoryBytesToKeepFree > memory.maxMemory()) {
            throw new IllegalArgumentException("This state store provider has been configured to keep at least " +
                    displayBytesToKeepFree + " of heap available, but the maximum allowed heap size is only " +
                    FileUtils.byteCountToDisplaySize(memory.maxMemory()) + "!");
        }
        LOGGER.debug("Keeping {} free. Found memory use: {}", displayBytesToKeepFree, memory);
        while (memory.availableMemory() < memoryBytesToKeepFree) {
            LOGGER.info("Removing old state stores from cache. Keeping {} free, found: {}", displayBytesToKeepFree, memory);
            if (!removeLeastRecentlyUsedStateStoreFromCache(requiredTableIds)) {
                LOGGER.warn("Could not free up memory because no table was available to be removed from the cache");
                return false;
            }
            memoryProvider.gc();
            memory = memoryProvider.getMemory();
            LOGGER.info("Memory now available: {}", memory);
        }
        return true;
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

    private boolean removeLeastRecentlyUsedStateStoreFromCache(Collection<String> requiredTableIds) {

        Optional<String> tableIdToRemove = tableIds.stream()
                .filter(tableId -> !requiredTableIds.contains(tableId))
                .findFirst();

        if (tableIdToRemove.isPresent()) {
            removeStateStoreFromCache(tableIdToRemove.get());
        }
        return tableIdToRemove.isPresent();
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
