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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SleeperProperty;
import sleeper.configuration.properties.instance.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.SleeperVersion;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.datageneration.GenerateNumberedRecords;
import sleeper.systemtest.datageneration.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.instance.SleeperInstanceTables;
import sleeper.systemtest.dsl.instance.SleeperInstanceTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.TAGS;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public final class SleeperInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstance.class);

    private final String instanceId;
    private final DeployInstanceConfiguration configuration;
    private final InstanceProperties instanceProperties = new InstanceProperties();
    private final SleeperInstanceTables tables;
    private GenerateNumberedValueOverrides generatorOverrides = GenerateNumberedValueOverrides.none();

    public SleeperInstance(String instanceId, DeployInstanceConfiguration configuration, SleeperInstanceTablesDriver tablesDriver) {
        this.instanceId = instanceId;
        this.configuration = configuration;
        this.tables = new SleeperInstanceTables(instanceProperties, tablesDriver);
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public Stream<Record> generateNumberedRecords(Schema schema, LongStream numbers) {
        return GenerateNumberedRecords.from(schema, generatorOverrides, numbers);
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        this.generatorOverrides = overrides;
    }

    public void loadOrDeployIfNeeded(
            SystemTestParameters parameters, SystemTestDeploymentContext systemTest,
            SleeperInstanceDriver driver) {
        boolean newInstance = driver.deployInstanceIfNotPresent(instanceId, configuration);
        driver.loadInstanceProperties(instanceProperties, instanceId);
        if (!newInstance && isRedeployNeeded(parameters, systemTest)) {
            redeploy(driver);
        }
    }

    public void redeploy(SleeperInstanceDriver driver) {
        driver.redeploy(instanceProperties, tables.getTablePropertiesProvider()
                .streamAllTables().collect(toUnmodifiableList()));
    }

    public void resetInstanceProperties(SleeperInstanceDriver driver) {
        ResetProperties.reset(instanceProperties, configuration.getInstanceProperties());
        driver.saveInstanceProperties(instanceProperties);
    }

    public void addTablesFromDeployConfig(SleeperInstanceTablesDriver tablesDriver) {
        tables.addTables(tablesDriver, configuration.getTableProperties().stream()
                .map(deployProperties -> {
                    TableProperties properties = TableProperties.copyOf(deployProperties);
                    properties.set(TABLE_NAME, UUID.randomUUID().toString());
                    return properties;
                })
                .collect(toUnmodifiableList()));
    }

    public void deleteTables(SleeperInstanceTablesDriver driver) {
        tables.deleteAll(driver);
    }

    public SleeperInstanceTables tables() {
        return tables;
    }

    private boolean isRedeployNeeded(SystemTestParameters parameters,
                                     SystemTestDeploymentContext systemTest) {
        boolean redeployNeeded = false;

        Set<String> ingestRoles = new LinkedHashSet<>(instanceProperties.getList(INGEST_SOURCE_ROLE));
        if (systemTest.isSystemTestClusterEnabled() &&
                !ingestRoles.contains(systemTest.getSystemTestWriterRoleName())) {
            ingestRoles.add(systemTest.getSystemTestWriterRoleName());
            instanceProperties.set(INGEST_SOURCE_ROLE, String.join(",", ingestRoles));
            redeployNeeded = true;
            LOGGER.info("Redeploy required to give system test cluster access to the instance");
        }

        if (!SleeperVersion.getVersion().equals(instanceProperties.get(VERSION))) {
            redeployNeeded = true;
            LOGGER.info("Redeploy required as version number does not match");
        }

        if (isRedeployDueToPropertyChange(UserDefinedInstanceProperty.getAll(),
                configuration.getInstanceProperties(), instanceProperties)) {
            redeployNeeded = true;
        }

        if (parameters.isForceRedeployInstances()) {
            LOGGER.info("Forcing redeploy");
            redeployNeeded = true;
        }
        return redeployNeeded;
    }

    private static <P extends SleeperProperty, T extends SleeperProperties<P>> boolean isRedeployDueToPropertyChange(
            List<? extends P> userDefinedProperties, T deployProperties, T foundProperties) {
        boolean redeployNeeded = false;
        for (P property : userDefinedProperties) {
            if (!property.isEditable() || !property.isRunCdkDeployWhenChanged()) {
                // Non-CDK properties get reset before every test in SleeperInstanceContext.resetProperties
                continue;
            }
            if (!deployProperties.isSet(property) || property == TAGS) {
                continue;
            }
            String deployValue = deployProperties.get(property);
            String foundValue = foundProperties.get(property);
            if (!foundProperties.isSet(property) || !Objects.equals(deployValue, foundValue)) {
                foundProperties.set(property, deployValue);
                LOGGER.info("Redeploy required as property changed: {}", property);
                LOGGER.info("Required value: {}", deployValue);
                LOGGER.info("Found value: {}", foundValue);
                redeployNeeded = true;
            }
        }
        return redeployNeeded;
    }
}
