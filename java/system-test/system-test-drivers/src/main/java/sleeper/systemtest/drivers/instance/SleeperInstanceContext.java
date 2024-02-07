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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.deploy.DeployInstanceConfiguration;
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
import sleeper.systemtest.datageneration.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.instance.InstanceDidNotDeployException;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.HashMap;
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
    private final SystemTestDeploymentContext systemTest;
    private final SleeperInstanceDriver instanceDriver;
    private final AwsSleeperInstanceTablesDriver tablesDriver;
    private final DeployedInstances deployed = new DeployedInstances();
    private SleeperInstance currentInstance;

    public SleeperInstanceContext(SystemTestParameters parameters, SystemTestDeploymentContext systemTest,
                                  AmazonDynamoDB dynamoDB, AmazonS3 s3, S3Client s3v2,
                                  AWSSecurityTokenService sts, AwsRegionProvider regionProvider,
                                  CloudFormationClient cloudFormationClient, AmazonECR ecr) {
        this.parameters = parameters;
        this.systemTest = systemTest;
        this.instanceDriver = new SleeperInstanceDriver(parameters, dynamoDB, s3, s3v2, sts, regionProvider, cloudFormationClient, ecr);
        this.tablesDriver = new AwsSleeperInstanceTablesDriver(s3, s3v2, dynamoDB, new Configuration());
    }

    public void connectTo(SystemTestInstanceConfiguration configuration) {
        currentInstance = deployed.connectTo(configuration);
        currentInstance.setGeneratorOverrides(GenerateNumberedValueOverrides.none());
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

    public void unsetTableProperties(List<TableProperty> properties) {
        streamTableProperties().forEach(tableProperties -> {
            properties.forEach(tableProperties::unset);
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
        return currentInstance.generateNumberedRecords(schema, numbers);
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
        currentInstance.setGeneratorOverrides(overrides);
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

    private class DeployedInstances {
        private final Map<String, Exception> failureById = new HashMap<>();
        private final Map<String, SleeperInstance> instanceById = new HashMap<>();

        public SleeperInstance connectTo(SystemTestInstanceConfiguration configuration) {
            String identifier = configuration.getIdentifier();
            if (failureById.containsKey(identifier)) {
                throw new InstanceDidNotDeployException(identifier, failureById.get(identifier));
            }
            try {
                return instanceById.computeIfAbsent(identifier,
                        id -> createInstanceIfMissing(id, configuration));
            } catch (RuntimeException e) {
                failureById.put(identifier, e);
                throw e;
            }
        }
    }

    private SleeperInstance createInstanceIfMissing(String identifier, SystemTestInstanceConfiguration configuration) {
        String instanceId = parameters.buildInstanceId(identifier);
        OutputInstanceIds.addInstanceIdToOutput(instanceId, parameters);
        DeployInstanceConfiguration deployConfig = configuration.buildDeployConfig(parameters, systemTest);
        SleeperInstance instance = new SleeperInstance(instanceId, deployConfig, tablesDriver);
        instance.loadOrDeployIfNeeded(parameters, systemTest, instanceDriver);
        return instance;
    }

}
