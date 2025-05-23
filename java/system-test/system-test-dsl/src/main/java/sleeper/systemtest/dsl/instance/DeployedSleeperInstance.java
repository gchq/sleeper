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

package sleeper.systemtest.dsl.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.SleeperVersion;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.UserDefinedInstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.dsl.SystemTestDrivers;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.TAGS;

public final class DeployedSleeperInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployedSleeperInstance.class);

    private final DeployInstanceConfiguration configuration;
    private final InstanceProperties instanceProperties;
    private final SystemTestDrivers instanceAdminDrivers;

    private DeployedSleeperInstance(
            DeployInstanceConfiguration configuration, InstanceProperties instanceProperties,
            SystemTestDrivers instanceAdminDrivers) {
        this.configuration = configuration;
        this.instanceProperties = instanceProperties;
        this.instanceAdminDrivers = instanceAdminDrivers;
    }

    public static DeployedSleeperInstance loadOrDeployAtFirstConnect(
            String instanceId, SystemTestInstanceConfiguration configuration,
            SystemTestParameters parameters, DeployedSystemTestResources systemTest,
            SleeperInstanceDriver driver, AssumeAdminRoleDriver assumeRoleDriver, ScheduleRulesDriver schedulesDriver) {
        DeployInstanceConfiguration deployConfig = configuration.buildDeployConfig(parameters, systemTest);
        boolean newInstance = driver.deployInstanceIfNotPresent(instanceId, deployConfig);

        InstanceProperties instanceProperties = new InstanceProperties();
        driver.loadInstanceProperties(instanceProperties, instanceId);

        DeployedSleeperInstance instance = new DeployedSleeperInstance(
                deployConfig, instanceProperties,
                assumeRoleDriver.assumeAdminRole(instanceProperties));
        if (!newInstance && instance.isRedeployNeeded(parameters, systemTest)) {
            instance.redeploy(driver, parameters);
        }
        driver.resetOnFirstConnect(instanceProperties);
        instance.resetInstanceProperties(driver);

        SleeperScheduleRule.getDeployedRules(instanceProperties).forEach(rule -> {
            if (configuration.isEnable(rule)) {
                schedulesDriver.enableRule(rule);
            } else {
                schedulesDriver.disableRule(rule);
            }
        });
        return instance;
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public SystemTestDrivers getInstanceAdminDrivers() {
        return instanceAdminDrivers;
    }

    public void redeploy(SleeperInstanceDriver driver, SystemTestParameters parameters) {
        driver.redeploy(instanceProperties,
                instanceAdminDrivers.tables(parameters).createTablePropertiesProvider(instanceProperties)
                        .streamAllTables().collect(toUnmodifiableList()));
    }

    private void resetInstanceProperties(SleeperInstanceDriver driver) {
        ResetProperties.reset(instanceProperties, configuration.getInstanceProperties());
        driver.saveInstanceProperties(instanceProperties);
    }

    public List<TableProperties> getDefaultTables() {
        return configuration.getTableProperties();
    }

    private boolean isRedeployNeeded(SystemTestParameters parameters, DeployedSystemTestResources systemTest) {
        boolean redeployNeeded = false;

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
