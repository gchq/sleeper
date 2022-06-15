/*
 * Copyright 2022 Crown Copyright
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
package sleeper.table.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import java.util.HashMap;
import java.util.Map;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class StateStoreProvider {
    private final StateStoreFactory stateStoreFactory;
    private final Map<String, StateStore> tableNameToStateStoreCache;

    public StateStoreProvider(AmazonDynamoDB dynamoDBClient,
                              InstanceProperties instanceProperties,
                              Configuration configuration) {
        this.stateStoreFactory = new StateStoreFactory(dynamoDBClient, instanceProperties, configuration);
        this.tableNameToStateStoreCache = new HashMap<>();
    }

    public StateStoreProvider(AmazonDynamoDB dynamoDBClient,
                              InstanceProperties instanceProperties) {
        this(dynamoDBClient, instanceProperties, null);
    }

    public StateStore getStateStore(String tableName, TablePropertiesProvider tablePropertiesProvider) {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        return getStateStore(tableProperties);
    }

    public StateStore getStateStore(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        if (!tableNameToStateStoreCache.containsKey(tableName)) {
            StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
            tableNameToStateStoreCache.put(tableName, stateStore);
        }
        return tableNameToStateStoreCache.get(tableName);
    }
}
