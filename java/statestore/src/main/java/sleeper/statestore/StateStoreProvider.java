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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * Caches Sleeper table state store objects. An instance of this class cannot be used concurrently in multiple threads,
 * as the cache is not thread-safe.
 */
public class StateStoreProvider {
    private final Function<TableProperties, StateStore> stateStoreFactory;
    private final Map<String, StateStore> tableNameToStateStoreCache;

    public StateStoreProvider(
            InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, Configuration configuration) {
        this(new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, configuration)::getStateStore);
    }

    protected StateStoreProvider(Function<TableProperties, StateStore> stateStoreFactory) {
        this.stateStoreFactory = stateStoreFactory;
        this.tableNameToStateStoreCache = new HashMap<>();
    }

    public StateStore getStateStore(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        if (!tableNameToStateStoreCache.containsKey(tableName)) {
            StateStore stateStore = stateStoreFactory.apply(tableProperties);
            tableNameToStateStoreCache.put(tableName, stateStore);
        }
        return tableNameToStateStoreCache.get(tableName);
    }
}
