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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.jboss.threads.ArrayQueue;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;

import java.util.HashMap;
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
    private final StateStoreLoader stateStoreFactory;
    private final Map<String, StateStore> tableIdToStateStoreCache;
    private final Queue<String> tableIds;

    public StateStoreProvider(
            InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Configuration configuration) {
        this(instanceProperties, new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, configuration)::getStateStore);
    }

    public StateStoreProvider(InstanceProperties instanceProperties, StateStoreLoader stateStoreFactory) {
        this(instanceProperties.getInt(STATESTORE_PROVIDER_CACHE_SIZE), stateStoreFactory);
    }

    protected StateStoreProvider(int cacheSize, StateStoreLoader stateStoreFactory) {
        this.cacheSize = cacheSize;
        this.stateStoreFactory = stateStoreFactory;
        this.tableIdToStateStoreCache = new HashMap<>();
        this.tableIds = new ArrayQueue<>(cacheSize);
    }

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

    public interface StateStoreLoader {
        StateStore getStateStore(TableProperties tableProperties);
    }
}
