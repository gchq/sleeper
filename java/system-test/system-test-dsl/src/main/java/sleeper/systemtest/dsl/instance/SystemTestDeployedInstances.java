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

import sleeper.configuration.deploy.DeployInstanceConfiguration;

import java.util.HashMap;
import java.util.Map;

class SystemTestDeployedInstances {
    private final SystemTestParameters parameters;
    private final SystemTestDeploymentContext systemTest;
    private final SleeperInstanceDriver instanceDriver;
    private final SleeperInstanceTablesDriver tablesDriver;
    private final Map<String, Exception> failureById = new HashMap<>();
    private final Map<String, SleeperInstance> instanceById = new HashMap<>();

    public SystemTestDeployedInstances(SystemTestParameters parameters,
                                       SystemTestDeploymentContext systemTest,
                                       SleeperInstanceDriver instanceDriver,
                                       SleeperInstanceTablesDriver tablesDriver) {
        this.parameters = parameters;
        this.systemTest = systemTest;
        this.instanceDriver = instanceDriver;
        this.tablesDriver = tablesDriver;
    }

    public SleeperInstance connectTo(SystemTestInstanceConfiguration configuration) {
        String instanceName = configuration.getShortName();
        if (failureById.containsKey(instanceName)) {
            throw new InstanceDidNotDeployException(instanceName, failureById.get(instanceName));
        }
        try {
            return instanceById.computeIfAbsent(instanceName,
                    id -> createInstanceIfMissingAndReset(id, configuration));
        } catch (RuntimeException e) {
            failureById.put(instanceName, e);
            throw e;
        }
    }

    private SleeperInstance createInstanceIfMissingAndReset(String identifier, SystemTestInstanceConfiguration configuration) {
        String instanceId = parameters.buildInstanceId(identifier);
        OutputInstanceIds.addInstanceIdToOutput(instanceId, parameters);
        DeployInstanceConfiguration deployConfig = configuration.buildDeployConfig(parameters, systemTest);
        SleeperInstance instance = new SleeperInstance(instanceId, deployConfig);
        instance.loadOrDeployIfNeeded(parameters, systemTest, instanceDriver, tablesDriver);
        instance.resetInstanceProperties(instanceDriver);
        tablesDriver.deleteAllTables(instance.getInstanceProperties());
        return instance;
    }
}
