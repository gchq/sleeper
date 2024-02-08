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

package sleeper.systemtest.dsl.testutil;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.InMemoryTableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.dsl.instance.SleeperInstanceTablesDriver;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreUninitialised;

public class InMemorySleeperInstanceTablesDriver implements SleeperInstanceTablesDriver {

    private final Map<String, TableIndex> tableIndexByInstanceId = new TreeMap<>();
    private final Map<String, TablePropertiesStore> propertiesStoreByInstanceId = new TreeMap<>();
    private final Map<String, Map<String, StateStore>> stateStoresByInstanceId = new TreeMap<>();

    @Override
    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        deployedInstancePropertiesStore(instanceProperties).save(tableProperties);
    }

    @Override
    public void deleteAllTables(InstanceProperties instanceProperties) {
        TablePropertiesStore tables = deployedInstancePropertiesStore(instanceProperties);
        tables.streamAllTableIds().forEach(tables::delete);
    }

    @Override
    public void addTable(InstanceProperties instanceProperties, TableProperties properties) {
        deployedInstancePropertiesStore(instanceProperties).createTable(properties);
        stateStoresByInstanceId.get(instanceProperties.get(ID))
                .put(properties.get(TABLE_ID), inMemoryStateStoreUninitialised(properties.getSchema()));
    }

    @Override
    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties) {
        return new TablePropertiesProvider(instanceProperties, deployedInstancePropertiesStore(instanceProperties), Instant::now);
    }

    @Override
    public StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        Map<String, StateStore> stateStores = stateStoresByInstanceId.get(instanceId);
        if (stateStores == null) {
            throw new IllegalArgumentException("Instance not found: " + instanceId);
        }
        return new FixedStateStoreProvider(stateStores);
    }

    @Override
    public TableIndex tableIndex(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        TableIndex tableIndex = tableIndexByInstanceId.get(instanceId);
        if (tableIndex == null) {
            throw new IllegalArgumentException("Instance not found: " + instanceId);
        }
        return tableIndex;
    }

    public void addInstance(String instanceId) {
        TableIndex tableIndex = new InMemoryTableIndex();
        tableIndexByInstanceId.put(instanceId, tableIndex);
        propertiesStoreByInstanceId.put(instanceId, InMemoryTableProperties.getStore(tableIndex));
        stateStoresByInstanceId.put(instanceId, new TreeMap<>());
    }

    private TablePropertiesStore deployedInstancePropertiesStore(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        TablePropertiesStore tables = propertiesStoreByInstanceId.get(instanceId);
        if (tables == null) {
            throw new IllegalArgumentException("Instance not found: " + instanceId);
        }
        return tables;
    }
}
