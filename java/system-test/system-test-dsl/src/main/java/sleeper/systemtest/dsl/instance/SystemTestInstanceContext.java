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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableIdentity;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedRecords;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SystemTestInstanceContext {
    private final SystemTestParameters parameters;
    private final DeployedSleeperInstances deployedInstances;
    private final SleeperInstanceDriver instanceDriver;
    private final SleeperTablesDriver tablesDriver;
    private final Map<String, DeployedSleeperTablesForTest> tablesByInstanceShortName = new HashMap<>();
    private DeployedSleeperInstance currentInstance = null;
    private DeployedSleeperTablesForTest currentTables = null;
    private GenerateNumberedValueOverrides generatorOverrides = null;

    public SystemTestInstanceContext(SystemTestParameters parameters,
                                     DeployedSleeperInstances deployedInstances,
                                     SleeperInstanceDriver instanceDriver,
                                     SleeperTablesDriver tablesDriver) {
        this.parameters = parameters;
        this.deployedInstances = deployedInstances;
        this.instanceDriver = instanceDriver;
        this.tablesDriver = tablesDriver;
    }

    public void connectTo(SystemTestInstanceConfiguration configuration) {
        currentInstance = deployedInstances.connectToAndReset(configuration);
        currentTables = tablesByInstanceShortName.computeIfAbsent(configuration.getShortName(),
                name -> new DeployedSleeperTablesForTest(currentInstance.getInstanceProperties(), tablesDriver));
    }

    public void addDefaultTables() {
        currentTables.addTables(tablesDriver, currentInstance.getDefaultTables().stream()
                .map(deployProperties -> {
                    TableProperties properties = TableProperties.copyOf(deployProperties);
                    properties.set(TABLE_NAME, UUID.randomUUID().toString());
                    return properties;
                })
                .collect(toUnmodifiableList()));
    }

    public void redeployCurrentInstance() {
        currentInstance.redeploy(instanceDriver, tablesDriver);
    }

    public InstanceProperties getInstanceProperties() {
        return currentInstance.getInstanceProperties();
    }

    public TableProperties getTableProperties() {
        return currentTables.getTableProperties();
    }

    public Optional<TableProperties> getTablePropertiesByName(String tableName) {
        return currentTables.getTablePropertiesByName(tableName);
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return currentTables.getTablePropertiesProvider();
    }

    public void updateTableProperties(Map<TableProperty, String> values) {
        List<TableProperty> uneditableProperties = values.keySet().stream()
                .filter(not(TableProperty::isEditable))
                .collect(Collectors.toUnmodifiableList());
        if (!uneditableProperties.isEmpty()) {
            throw new IllegalArgumentException("Cannot edit properties: " + uneditableProperties);
        }
        streamTableProperties().forEach(tableProperties -> {
            values.forEach(tableProperties::set);
            tablesDriver.saveTableProperties(getInstanceProperties(), tableProperties);
        });
    }

    public StateStoreProvider getStateStoreProvider() {
        return currentTables.getStateStoreProvider();
    }

    public Stream<Record> generateNumberedRecords(LongStream numbers) {
        return generateNumberedRecords(currentTables.getSchema(), numbers);
    }

    public Stream<Record> generateNumberedRecords(Schema schema, LongStream numbers) {
        return GenerateNumberedRecords.from(schema, generatorOverrides, numbers);
    }

    public StateStore getStateStore() {
        return getStateStore(getTableProperties());
    }

    public StateStore getStateStore(TableProperties tableProperties) {
        return getStateStoreProvider().getStateStore(tableProperties);
    }

    public String getTableName() {
        return getTableProperties().get(TABLE_NAME);
    }

    public TableIdentity getTableId() {
        return getTableProperties().getId();
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        generatorOverrides = overrides;
    }

    public void createTables(int numberOfTables, Schema schema, Map<TableProperty, String> setProperties) {
        InstanceProperties instanceProperties = getInstanceProperties();
        currentTables.addTables(tablesDriver, IntStream.range(0, numberOfTables)
                .mapToObj(i -> {
                    TableProperties tableProperties = parameters.createTableProperties(instanceProperties, schema);
                    setProperties.forEach(tableProperties::set);
                    return tableProperties;
                })
                .collect(Collectors.toUnmodifiableList()));
    }

    public List<TableIdentity> loadTableIdentities() {
        return tablesDriver.tableIndex(getInstanceProperties()).streamAllTables()
                .collect(Collectors.toUnmodifiableList());
    }

    public Stream<String> streamTableNames() {
        return currentTables.streamTableNames();
    }

    public Stream<TableProperties> streamTableProperties() {
        return currentTables.streamTableProperties();
    }

}
