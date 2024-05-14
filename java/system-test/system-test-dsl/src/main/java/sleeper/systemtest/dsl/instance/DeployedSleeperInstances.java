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

import java.util.HashMap;
import java.util.Map;

public class DeployedSleeperInstances {
    private final SystemTestParameters parameters;
    private final DeployedSystemTestResources systemTest;
    private final SleeperInstanceDriver instanceDriver;
    private final AssumeAdminRoleDriver assumeRoleDriver;
    private final Map<String, Exception> failureById = new HashMap<>();
    private final Map<String, DeployedSleeperInstance> instanceByShortName = new HashMap<>();

    public DeployedSleeperInstances(
            SystemTestParameters parameters, DeployedSystemTestResources systemTest,
            SleeperInstanceDriver instanceDriver, AssumeAdminRoleDriver assumeRoleDriver) {
        this.parameters = parameters;
        this.systemTest = systemTest;
        this.instanceDriver = instanceDriver;
        this.assumeRoleDriver = assumeRoleDriver;
    }

    public DeployedSleeperInstance connectToAndReset(SystemTestInstanceConfiguration configuration) {
        String instanceShortName = configuration.getShortName();
        if (failureById.containsKey(instanceShortName)) {
            throw new InstanceDidNotDeployException(instanceShortName, failureById.get(instanceShortName));
        }
        try {
            DeployedSleeperInstance instance = instanceByShortName.computeIfAbsent(instanceShortName,
                    name -> createInstanceIfMissing(name, configuration));
            instance.resetInstanceProperties(instanceDriver);
            instance.getInstanceAdminDrivers().tables(parameters)
                    .deleteAllTables(instance.getInstanceProperties());
            return instance;
        } catch (RuntimeException e) {
            failureById.put(instanceShortName, e);
            throw e;
        }
    }

    private DeployedSleeperInstance createInstanceIfMissing(String identifier, SystemTestInstanceConfiguration configuration) {
        String instanceId = parameters.buildInstanceId(identifier);
        OutputInstanceIds.addInstanceIdToOutput(instanceId, parameters);
        return DeployedSleeperInstance.loadOrDeployIfNeeded(
                instanceId, configuration, parameters, systemTest, instanceDriver, assumeRoleDriver);
    }
}
