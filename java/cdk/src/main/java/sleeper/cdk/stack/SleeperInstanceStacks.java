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
package sleeper.cdk.stack;

import software.constructs.Construct;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.stack.core.PropertiesStack;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;

public class SleeperInstanceStacks {

    private SleeperInstanceStacks() {
    }

    public static void create(Construct scope, DeployInstanceConfiguration configuration, SleeperJarsInBucket jars) {
        InstanceProperties instanceProperties = configuration.getInstanceProperties();

        SleeperCoreStacks coreStacks = SleeperCoreStacks.create(scope, instanceProperties, jars);
        SleeperOptionalStacks.create(scope, instanceProperties, configuration.getTableProperties(), jars, coreStacks);

        // Only create roles after we know which policies are deployed in the instance
        coreStacks.createRoles();
        // Only write properties after CDK-defined properties are set
        new PropertiesStack(scope, "Properties", instanceProperties, jars, coreStacks);
    }

}
