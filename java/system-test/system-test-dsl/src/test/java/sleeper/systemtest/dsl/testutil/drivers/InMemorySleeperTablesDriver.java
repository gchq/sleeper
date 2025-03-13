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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class InMemorySleeperTablesDriver implements SleeperTablesDriver {

    private final Map<String, TableIndex> tableIndexByInstanceId = new TreeMap<>();
    private final Map<String, TablePropertiesStore> propertiesStoreByInstanceId = new TreeMap<>();
    private final Map<String, Map<String, StateStore>> stateStoresByInstanceId = new TreeMap<>();
    private final InMemoryTransactionLogsPerTable transactionLogs;

    public InMemorySleeperTablesDriver(InMemoryTransactionLogsPerTable transactionLogs) {
        this.transactionLogs = transactionLogs;
    }

    @Override
    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        deployedInstancePropertiesStore(instanceProperties.get(ID)).save(tableProperties);
    }

    /**
     * Note that this is synchronized because this is called in parallel in DeployedSleeperTablesForTest.addTables.
     */
    @Override
    public synchronized void addTable(InstanceProperties instanceProperties, TableProperties properties) {
        String instanceId = instanceProperties.get(ID);
        properties.validate();
        addInstanceIfNotPresent(instanceId);
        deployedInstancePropertiesStore(instanceId).createTable(properties);
        StateStore stateStore = transactionLogs.stateStoreBuilder(properties).build();
        update(stateStore).initialise(properties.getSchema());
        stateStoresByInstanceId.get(instanceId)
                .put(properties.get(TABLE_NAME), stateStore);
    }

    @Override
    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        addInstanceIfNotPresent(instanceId);
        return new TablePropertiesProvider(instanceProperties, propertiesStoreByInstanceId.get(instanceId), Instant::now);
    }

    @Override
    public StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        addInstanceIfNotPresent(instanceId);
        return FixedStateStoreProvider.byTableName(stateStoresByInstanceId.get(instanceId));
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

    private void addInstanceIfNotPresent(String instanceId) {
        if (tableIndexByInstanceId.containsKey(instanceId)) {
            return;
        }
        TableIndex tableIndex = new InMemoryTableIndex();
        tableIndexByInstanceId.put(instanceId, tableIndex);
        propertiesStoreByInstanceId.put(instanceId, InMemoryTableProperties.getStore(tableIndex));
        stateStoresByInstanceId.put(instanceId, new TreeMap<>());
    }

    private TablePropertiesStore deployedInstancePropertiesStore(String instanceId) {
        TablePropertiesStore tables = propertiesStoreByInstanceId.get(instanceId);
        if (tables == null) {
            throw new IllegalArgumentException("Instance not found: " + instanceId);
        }
        return tables;
    }
}
