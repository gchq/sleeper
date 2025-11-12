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
package sleeper.cdk;

import software.constructs.Construct;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.SleeperOptionalStacks;
import sleeper.cdk.stack.core.PropertiesStack;
import sleeper.cdk.stack.core.SleeperInstanceRoles;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks. Does not create Sleeper tables. If the
 * configuration for tables is provided then the optional DashboardStack will create individual dashboards for each
 * table.
 */
public class SleeperInstance {

    private final SleeperCoreStacks coreStacks;

    private SleeperInstance(SleeperCoreStacks coreStacks) {
        this.coreStacks = coreStacks;
    }

    public SleeperInstanceRoles getRoles() {
        return coreStacks.getRoles();
    }

    /**
     * Adds all nested stacks for an instance of Sleeper at a given scope. This should only be used at a scope with no
     * other constructs, as the IDs of the nested stack constructs are not namespaced.
     *
     * @param  scope the scope of the Sleeper instance
     * @param  props configuration to deploy the instance
     * @return       the Sleeper instance
     */
    public static SleeperInstance addNestedStacks(Construct scope, SleeperInstanceProps props) {

        SleeperCoreStacks coreStacks = SleeperCoreStacks.create(scope, props);
        SleeperOptionalStacks.create(scope, props, coreStacks);

        // Only create roles after we know which policies are deployed in the instance
        coreStacks.createRoles();
        // Only write properties after CDK-defined properties are set
        new PropertiesStack(scope, "Properties", props.getInstanceProperties(), props.getJars(), coreStacks);
        return new SleeperInstance(coreStacks);
    }

}
