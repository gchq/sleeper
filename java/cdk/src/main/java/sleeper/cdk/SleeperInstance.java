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

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.NestedStackProps;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.constructs.Construct;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.SleeperOptionalStacks;
import sleeper.cdk.stack.core.PropertiesStack;
import sleeper.cdk.stack.core.SleeperInstanceRoles;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks.
 * <p>
 * Does not create Sleeper tables. If the configuration for tables is provided then the optional DashboardStack will
 * create individual dashboards for each table.
 */
public class SleeperInstance {

    private final InstanceProperties instanceProperties;
    private final SleeperCoreStacks coreStacks;

    private SleeperInstance(InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
        this.instanceProperties = instanceProperties;
        this.coreStacks = coreStacks;
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public SleeperInstanceRoles getRoles() {
        return coreStacks.getRoles();
    }

    /**
     * Declares an instance of Sleeper as a nested stack.
     *
     * @param  scope        the scope to add the instance to, usually a Stack or NestedStack
     * @param  id           the nested stack ID
     * @param  stackProps   configuration of the nested stack
     * @param  sleeperProps configuration to deploy the instance
     * @return              a reference to interact with constructs in the Sleeper instance
     */
    public static SleeperInstance createAsNestedStack(Construct scope, String id, NestedStackProps stackProps, SleeperInstanceProps sleeperProps) {
        NestedStack stack = new NestedStack(scope, id, stackProps);
        return create(stack, sleeperProps);
    }

    /**
     * Declares an instance of Sleeper as a root level stack.
     *
     * @param  scope        the scope to add the instance to, usually an App or Stage
     * @param  id           the stack ID
     * @param  stackProps   configuration of the stack
     * @param  sleeperProps configuration to deploy the instance
     * @return              the stack
     */
    public static Stack createAsRootStack(Construct scope, String id, StackProps stackProps, SleeperInstanceProps sleeperProps) {
        Stack stack = new Stack(scope, id, stackProps);
        create(stack, sleeperProps);
        return stack;
    }

    /**
     * Declares an instance of Sleeper as a stack. Takes an empty stack that will contain the resources of the instance.
     * <p>
     * Please avoid adding other resources to the same stack, to avoid construct ID conflicts.
     * <p>
     * A Sleeper instance consists of a number of nested stacks, which will be nested under the stack you provide. These
     * nested stacks will always have the same CDK construct IDs. For multiple instances, each Sleeper instance should
     * be its own nested stack. Also see {@link #createAsNestedStack()}.
     *
     * @param  stack an empty stack to contain the instance resources, usually a Stack or NestedStack
     * @param  props configuration to deploy the instance
     * @return       the stack
     */
    public static SleeperInstance create(Stack stack, SleeperInstanceProps props) {
        SleeperCoreStacks coreStacks = SleeperCoreStacks.create(stack, props);
        SleeperOptionalStacks.create(stack, props, coreStacks);

        // Only create roles after we know which policies are deployed in the instance
        coreStacks.createRoles();
        // Only write properties after CDK-defined properties are set
        new PropertiesStack(stack, "Properties", props.getInstanceProperties(), props.getJars(), coreStacks);
        return new SleeperInstance(props.getInstanceProperties(), coreStacks);
    }

}
