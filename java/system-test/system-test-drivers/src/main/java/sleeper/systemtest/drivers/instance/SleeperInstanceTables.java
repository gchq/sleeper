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

package sleeper.systemtest.drivers.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIndex;
import sleeper.statestore.StateStoreProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public final class SleeperInstanceTables {

    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceTables.class);
    private final InstanceProperties instanceProperties;
    private final SleeperInstanceTablesDriver driver;
    private final Map<String, TableProperties> tableByName = new TreeMap<>();
    private TablePropertiesProvider tablePropertiesProvider;
    private StateStoreProvider stateStoreProvider;
    private TableProperties currentTable;

    public SleeperInstanceTables(
            InstanceProperties instanceProperties,
            SleeperInstanceTablesDriver driver) {
        this.instanceProperties = instanceProperties;
        this.driver = driver;
        this.tablePropertiesProvider = driver.createTablePropertiesProvider(instanceProperties);
        this.stateStoreProvider = driver.createStateStoreProvider(instanceProperties);
        this.currentTable = null;
    }

    public void deleteAll() {
        String instanceId = instanceProperties.get(ID);
        LOGGER.info("Deleting all tables with instance ID: {}", instanceId);
        driver.deleteAll(instanceProperties);
        tablePropertiesProvider = driver.createTablePropertiesProvider(instanceProperties);
        stateStoreProvider = driver.createStateStoreProvider(instanceProperties);
        tableByName.clear();
        currentTable = null;
    }

    public void addTables(List<TableProperties> tables) {
        LOGGER.info("Adding {} tables with instance ID: {}", tables.size(), instanceProperties.get(ID));
        tables.stream().parallel().forEach(tableProperties ->
                driver.add(instanceProperties, tableProperties));
        tables.forEach(tableProperties ->
                tableByName.put(tableProperties.get(TABLE_NAME), tableProperties));
        if (tables.size() == 1) {
            currentTable = tables.get(0);
        }
    }

    public Optional<TableProperties> getTablePropertiesByName(String tableName) {
        return Optional.ofNullable(tableByName.get(tableName));
    }

    public TableProperties getTableProperties() {
        return currentTable;
    }

    public Schema getSchema() {
        return currentTable.getSchema();
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return tablePropertiesProvider;
    }

    public StateStoreProvider getStateStoreProvider() {
        return stateStoreProvider;
    }

    public TableIndex deployedIndex() {
        return driver.tableIndex(instanceProperties);
    }

    public Stream<String> streamTableNames() {
        return tableByName.keySet().stream();
    }

    public Stream<TableProperties> streamTableProperties() {
        return tableByName.values().stream();
    }
}
