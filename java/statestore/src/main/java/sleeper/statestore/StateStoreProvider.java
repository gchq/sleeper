/*
 * Copyright 2022-2023 Crown Copyright
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
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class StateStoreProvider {
    private final Function<TableProperties, StateStore> stateStoreFactory;
    private final Map<String, StateStore> tableNameToStateStoreCache;

    public StateStoreProvider(AmazonDynamoDB dynamoDBClient,
                              InstanceProperties instanceProperties,
                              Configuration configuration) {
        this(new StateStoreFactory(dynamoDBClient, instanceProperties, configuration)::getStateStore);
    }

    public StateStoreProvider(AmazonDynamoDB dynamoDBClient,
                              InstanceProperties instanceProperties) {
        this(dynamoDBClient, instanceProperties, null);
    }

    protected StateStoreProvider(Function<TableProperties, StateStore> stateStoreFactory) {
        this.stateStoreFactory = stateStoreFactory;
        this.tableNameToStateStoreCache = new HashMap<>();
    }

    public StateStore getStateStore(String tableName, TablePropertiesProvider tablePropertiesProvider) {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        return getStateStore(tableProperties);
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
