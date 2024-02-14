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
import sleeper.configuration.properties.instance.UserDefinedInstanceProperty;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SleeperInstanceContext {
    private final SystemTestParameters parameters;
    private final SleeperInstanceDriver instanceDriver;
    private final SleeperInstanceTablesDriver tablesDriver;
    private final SystemTestDeployedInstances deployed;
    private SleeperInstance currentInstance = null;
    private GenerateNumberedValueOverrides generatorOverrides = null;

    public SleeperInstanceContext(SystemTestParameters parameters, SystemTestDeploymentContext systemTest,
                                  SleeperInstanceDriver instanceDriver, SleeperInstanceTablesDriver tablesDriver) {
        this.parameters = parameters;
        this.instanceDriver = instanceDriver;
        this.tablesDriver = tablesDriver;
        this.deployed = new SystemTestDeployedInstances(parameters, systemTest, instanceDriver);
    }

    public void connectTo(SystemTestInstanceConfiguration configuration) {
        currentInstance = deployed.connectTo(configuration);
        generatorOverrides = GenerateNumberedValueOverrides.none();
    }

    public void disconnect() {
        currentInstance = null;
    }

    public void resetPropertiesAndTables() {
        currentInstance.resetInstanceProperties(instanceDriver);
        currentInstance.deleteTables(tablesDriver);
        currentInstance.addTablesFromDeployConfig(tablesDriver);
    }

    public void resetPropertiesAndDeleteTables() {
        currentInstance.resetInstanceProperties(instanceDriver);
        currentInstance.deleteTables(tablesDriver);
    }

    public void redeploy() {
        currentInstance.redeploy(instanceDriver);
    }

    public InstanceProperties getInstanceProperties() {
        return currentInstance.getInstanceProperties();
    }

    public TableProperties getTableProperties() {
        return currentInstance.tables().getTableProperties();
    }

    public Optional<TableProperties> getTablePropertiesByName(String tableName) {
        return currentInstance.tables().getTablePropertiesByName(tableName);
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return currentInstance.tables().getTablePropertiesProvider();
    }

    public void updateInstanceProperties(Map<UserDefinedInstanceProperty, String> values) {
        InstanceProperties instanceProperties = getInstanceProperties();
        values.forEach(instanceProperties::set);
        instanceDriver.saveInstanceProperties(instanceProperties);
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
        return currentInstance.tables().getStateStoreProvider();
    }

    public Stream<Record> generateNumberedRecords(LongStream numbers) {
        return generateNumberedRecords(currentInstance.tables().getSchema(), numbers);
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
        currentInstance.tables().addTables(tablesDriver, IntStream.range(0, numberOfTables)
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
        return currentInstance.tables().streamTableNames();
    }

    public Stream<TableProperties> streamTableProperties() {
        return currentInstance.tables().streamTableProperties();
    }

}
