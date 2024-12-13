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

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

public class DeployedSleeperInstances {
    private final SystemTestParameters parameters;
    private final DeployedSystemTestResources systemTest;
    private final SleeperInstanceDriver instanceDriver;
    private final AssumeAdminRoleDriver assumeRoleDriver;
    private final ScheduleRulesDriver schedulesDriver;
    private final Map<String, Exception> failureById = new HashMap<>();
    private final Map<String, DeployedSleeperInstance> instanceByShortName = new HashMap<>();

    public DeployedSleeperInstances(
            SystemTestParameters parameters, DeployedSystemTestResources systemTest,
            SleeperInstanceDriver instanceDriver, AssumeAdminRoleDriver assumeRoleDriver, ScheduleRulesDriver schedulesDriver) {
        this.parameters = parameters;
        this.systemTest = systemTest;
        this.instanceDriver = instanceDriver;
        this.assumeRoleDriver = assumeRoleDriver;
        this.schedulesDriver = schedulesDriver;
    }

    public DeployedSleeperInstance connectToAndReset(SystemTestInstanceConfiguration configuration) {
        String instanceShortName = configuration.getShortName();
        if (failureById.containsKey(instanceShortName)) {
            throw new InstanceDidNotDeployException(instanceShortName, failureById.get(instanceShortName));
        }
        try {
            return instanceByShortName.computeIfAbsent(instanceShortName,
                    name -> firstConnectInstance(name, configuration));
        } catch (RuntimeException e) {
            failureById.put(instanceShortName, e);
            throw e;
        }
    }

    private DeployedSleeperInstance firstConnectInstance(String identifier, SystemTestInstanceConfiguration configuration) {
        String instanceId = parameters.buildInstanceId(identifier);
        OutputInstanceIds.addInstanceIdToOutput(instanceId, parameters);
        DeployedSleeperInstance instance = loadOrDeployAtFirstConnect(instanceId, configuration);
        takeAllTablesOffline(instance);
        return instance;
    }

    private DeployedSleeperInstance loadOrDeployAtFirstConnect(String instanceId, SystemTestInstanceConfiguration configuration) {
        return DeployedSleeperInstance.loadOrDeployAtFirstConnect(
                instanceId, configuration, parameters, systemTest, instanceDriver, assumeRoleDriver, schedulesDriver);
    }

    private void takeAllTablesOffline(DeployedSleeperInstance instance) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        SleeperTablesDriver tablesDriver = instance.getInstanceAdminDrivers().tables(parameters);
        tablesDriver.createTablePropertiesProvider(instanceProperties)
                .streamOnlineTables().forEach(table -> {
                    table.set(TABLE_ONLINE, "false");
                    tablesDriver.saveTableProperties(instanceProperties, table);
                });
    }
}
