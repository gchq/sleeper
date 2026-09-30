/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.systemtest.dsl.SystemTestDrivers;

public class SystemTestDeploymentContext {

    private final SystemTestParameters parameters;
    private final SystemTestDrivers baseDrivers;
    private final DeployedSystemTestResources deployedResources;
    private final DeployedSleeperInstances deployedInstances;

    public SystemTestDeploymentContext(SystemTestParameters parameters, SystemTestDrivers baseDrivers) {
        this.parameters = parameters;
        this.baseDrivers = baseDrivers;
        this.deployedResources = new DeployedSystemTestResources(parameters, baseDrivers.systemTestDeployment(parameters));
        this.deployedInstances = new DeployedSleeperInstances(
                parameters, deployedResources, baseDrivers.instance(parameters), baseDrivers.assumeAdminRole(), baseDrivers.schedules());
    }

    public SystemTestParameters parameters() {
        return parameters;
    }

    /**
     * Returns drivers using the credentials of the system test process. These are used for deployment-scoped actions
     * that cannot be performed through an instance role. For operations on a connected instance, prefer that
     * instance's admin drivers so the system tests exercise the permissions granted to the instance admin role.
     *
     * @return the base system test drivers
     */
    public SystemTestDrivers baseDrivers() {
        return baseDrivers;
    }

    public DeployedSystemTestResources deployedResources() {
        return deployedResources;
    }

    public DeployedSleeperInstances deployedInstances() {
        return deployedInstances;
    }

}
