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

package sleeper.systemtest.dsl.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public final class DeployedSleeperTablesForTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeployedSleeperTablesForTest.class);
    private final InstanceProperties instanceProperties;
    private final Map<String, TableProperties> tableByName = new TreeMap<>();
    private final Map<String, TableProperties> tableById = new TreeMap<>();
    private final Map<String, StateStore> stateStoreByTableId = new TreeMap<>();
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private TableProperties currentTable = null;

    public DeployedSleeperTablesForTest(InstanceProperties instanceProperties, SleeperTablesDriver driver) {
        this.instanceProperties = instanceProperties;
        tablePropertiesProvider = driver.createTablePropertiesProvider(instanceProperties);
        stateStoreProvider = driver.createStateStoreProvider(instanceProperties);
    }

    public void addTablesAndSetCurrent(SleeperTablesDriver driver, List<TableProperties> tables) {
        addTables(driver, tables);
        if (tables.size() == 1) {
            currentTable = tables.get(0);
        } else {
            currentTable = null;
        }
    }

    public void addTables(SleeperTablesDriver driver, List<TableProperties> tables) {
        LOGGER.info("Adding {} tables with instance ID: {}", tables.size(), instanceProperties.get(ID));
        tables.stream().parallel().forEach(tableProperties -> driver.addTable(instanceProperties, tableProperties));
        tables.forEach(tableProperties -> {
            tableByName.put(tableProperties.get(TABLE_NAME), tableProperties);
            tableById.put(tableProperties.get(TABLE_ID), tableProperties);
            stateStoreByTableId.put(tableProperties.get(TABLE_ID), stateStoreProvider.getStateStore(tableProperties));
        });
    }

    public Optional<TableProperties> getTablePropertiesByName(String tableName) {
        return Optional.ofNullable(tableByName.get(tableName));
    }

    public Optional<TableProperties> getTablePropertiesById(String tableId) {
        return Optional.ofNullable(tableById.get(tableId));
    }

    public TableProperties getTableProperties() {
        return currentTable();
    }

    public Schema getSchema() {
        return currentTable().getSchema();
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return tablePropertiesProvider;
    }

    public StateStoreProvider getStateStoreProvider() {
        return stateStoreProvider;
    }

    public StateStore getStateStore(TableProperties tableProperties) {
        return stateStoreByTableId.get(tableProperties.get(TABLE_ID));
    }

    public Stream<String> streamTableNames() {
        return tableByName.keySet().stream();
    }

    public Stream<TableProperties> streamTableProperties() {
        return tableByName.values().stream();
    }

    public void setCurrent(TableProperties tableProperties) {
        currentTable = tableProperties;
    }

    private TableProperties currentTable() {
        return Optional.ofNullable(currentTable).orElseThrow(NoTableChosenException::new);
    }
}
