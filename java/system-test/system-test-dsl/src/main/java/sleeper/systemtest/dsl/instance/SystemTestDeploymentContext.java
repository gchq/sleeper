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

import sleeper.systemtest.dsl.SystemTestDrivers;

public class SystemTestDeploymentContext {

    private final SystemTestParameters parameters;
    private final SystemTestDrivers drivers;
    private final DeployedSystemTestResources deployedResources;
    private final DeployedSleeperInstances deployedInstances;

    public SystemTestDeploymentContext(SystemTestParameters parameters, SystemTestDrivers drivers) {
        this.parameters = parameters;
        this.drivers = drivers;
        this.deployedResources = new DeployedSystemTestResources(parameters, drivers.systemTestDeployment(parameters));
        this.deployedInstances = new DeployedSleeperInstances(
                parameters, deployedResources, drivers.instance(parameters), drivers.assumeAdminRole(), drivers.schedules());
    }

    public SystemTestParameters parameters() {
        return parameters;
    }

    public SystemTestDrivers drivers() {
        return drivers;
    }

    public DeployedSystemTestResources deployedResources() {
        return deployedResources;
    }

    public DeployedSleeperInstances deployedInstances() {
        return deployedInstances;
    }

}
