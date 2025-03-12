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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedRecords;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.util.TestContext;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

public class SystemTestInstanceContext {
    private final SystemTestParameters parameters;
    private final DeployedSleeperInstances deployedInstances;
    private final SleeperInstanceDriver instanceDriver;
    private final TestContext testContext;
    private final Map<String, DeployedSleeperTablesForTest> tablesByInstanceShortName = new HashMap<>();
    private final Map<String, TableProperties> tablesByTestName = new TreeMap<>();
    private final Map<String, String> testNameByTableId = new HashMap<>();
    private DeployedSleeperInstance currentInstance = null;
    private DeployedSleeperTablesForTest currentTables = null;
    private GenerateNumberedValueOverrides generatorOverrides = GenerateNumberedValueOverrides.none();

    public SystemTestInstanceContext(
            SystemTestParameters parameters, DeployedSleeperInstances deployedInstances,
            SleeperInstanceDriver instanceDriver, TestContext testContext) {
        this.parameters = parameters;
        this.deployedInstances = deployedInstances;
        this.instanceDriver = instanceDriver;
        this.testContext = testContext;
    }

    public void connectTo(SystemTestInstanceConfiguration configuration) {
        currentInstance = deployedInstances.connectToAndReset(configuration);
        currentTables = tablesByInstanceShortName.computeIfAbsent(configuration.getShortName(),
                name -> new DeployedSleeperTablesForTest(currentInstance.getInstanceProperties(), tablesDriver()));
    }

    public SystemTestDrivers adminDrivers() {
        return currentInstance().getInstanceAdminDrivers();
    }

    private SleeperTablesDriver tablesDriver() {
        return adminDrivers().tables(parameters);
    }

    public void addDefaultTables(Map<TableProperty, String> extraProperties) {
        currentTables().addTablesAndSetCurrent(tablesDriver(), currentInstance().getDefaultTables().stream()
                .map(deployProperties -> {
                    TableProperties properties = TableProperties.copyOf(deployProperties);
                    properties.unset(TABLE_ID);
                    properties.set(TABLE_NAME, buildTableName(properties.get(TABLE_NAME)));
                    extraProperties.forEach(properties::set);
                    return properties;
                }).collect(toUnmodifiableList()));
    }

    public void createTables(int numberOfTables, Schema schema, Map<TableProperty, String> setProperties) {
        InstanceProperties instanceProperties = getInstanceProperties();
        currentTables().addTablesAndSetCurrent(tablesDriver(), IntStream.range(0, numberOfTables)
                .mapToObj(i -> {
                    TableProperties tableProperties = parameters.createTableProperties(instanceProperties, schema);
                    setProperties.forEach(tableProperties::set);
                    return tableProperties;
                })
                .collect(toUnmodifiableList()));
    }

    public void createTable(String name, Schema schema, Map<TableProperty, String> setProperties) {
        TableProperties tableProperties = parameters.createTableProperties(getInstanceProperties(), schema);
        tableProperties.set(TABLE_NAME, buildTableName(name));
        setProperties.forEach(tableProperties::set);
        currentTables().addTablesAndSetCurrent(tablesDriver(), List.of(tableProperties));
        tablesByTestName.put(name, tableProperties);
        testNameByTableId.put(tableProperties.get(TABLE_ID), name);
    }

    public void setCurrentTable(String name) {
        TableProperties tableProperties = Optional.ofNullable(tablesByTestName.get(name)).orElseThrow();
        currentTables().setCurrent(tableProperties);
    }

    public void redeployCurrentInstance() {
        currentInstance().redeploy(instanceDriver, parameters);
    }

    public InstanceProperties getInstanceProperties() {
        return currentInstance().getInstanceProperties();
    }

    public TableProperties getTableProperties() {
        return currentTables().getTableProperties();
    }

    /**
     * Retrieves table properties by name. Table properties created by tests are stored in-memory, and can be retrieved
     * using this method. This avoids having to load them from S3 (as with {@link #getTablePropertiesProvider()}).
     *
     * @param  tableName the name of the table to load
     * @return           the table properties of the table
     */
    public Optional<TableProperties> getTablePropertiesByDeployedName(String tableName) {
        return currentTables().getTablePropertiesByName(tableName);
    }

    public Optional<TableProperties> getTablePropertiesByDeployedId(String tableId) {
        return currentTables().getTablePropertiesById(tableId);
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return currentTables().getTablePropertiesProvider();
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
            tablesDriver().saveTableProperties(getInstanceProperties(), tableProperties);
        });
    }

    public StateStoreProvider getStateStoreProvider() {
        return currentTables().getStateStoreProvider();
    }

    public GenerateNumberedRecords numberedRecords() {
        return numberedRecords(currentTables().getSchema());
    }

    public GenerateNumberedRecords numberedRecords(Schema schema) {
        return GenerateNumberedRecords.from(schema, generatorOverrides);
    }

    public StateStore getStateStore() {
        return getStateStore(getTableProperties());
    }

    public StateStore getStateStore(TableProperties tableProperties) {
        return currentTables().getStateStore(tableProperties);
    }

    public String getTableName() {
        return getTableProperties().get(TABLE_NAME);
    }

    public TableStatus getTableStatus() {
        return getTableProperties().getStatus();
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        generatorOverrides = overrides;
    }

    public List<TableStatus> loadTables() {
        TableIndex tableIndex = tablesDriver().tableIndex(getInstanceProperties());
        return streamTableProperties()
                .map(table -> tableIndex.getTableByUniqueId(table.get(TABLE_ID)).orElseThrow())
                .collect(toUnmodifiableList());
    }

    public Stream<String> streamDeployedTableNames() {
        return currentTables().streamTableNames();
    }

    public Stream<TableProperties> streamTableProperties() {
        return currentTablePropertiesCollection().stream();
    }

    public Collection<TableProperties> currentTablePropertiesCollection() {
        return currentTables().tablePropertiesCollection();
    }

    public void setCurrentTable(TableProperties tableProperties) {
        currentTables().setCurrent(tableProperties);
    }

    public String getTestTableName(String tableName) {
        return getTestTableName(getTablePropertiesByDeployedName(tableName).orElseThrow());
    }

    public String getTestTableName(TableProperties tableProperties) {
        return Optional.ofNullable(testNameByTableId.get(tableProperties.get(TABLE_ID)))
                .orElseGet(() -> tableProperties.get(TABLE_NAME));
    }

    public void takeTestTablesOfflineIfConnected() {
        if (currentTables == null) {
            return;
        }
        DeployedSleeperInstance instance = currentInstance();
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        SleeperTablesDriver tablesDriver = instance.getInstanceAdminDrivers().tables(parameters);
        currentTables.streamTableProperties().forEach(table -> {
            table.set(TABLE_ONLINE, "false");
            tablesDriver.saveTableProperties(instanceProperties, table);
        });
    }

    private DeployedSleeperInstance currentInstance() {
        return Optional.ofNullable(currentInstance).orElseThrow(NoInstanceConnectedException::new);
    }

    private DeployedSleeperTablesForTest currentTables() {
        return Optional.ofNullable(currentTables).orElseThrow(NoInstanceConnectedException::new);
    }

    private static final DateTimeFormatter TABLE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm", Locale.UK).withZone(ZoneOffset.UTC);

    private String buildTableName(String name) {
        return TABLE_TIME_FORMATTER.format(Instant.now()) + "-" +
                testContext.getTestClassAndMethod() + "-" +
                name + "-" + UUID.randomUUID();
    }
}
