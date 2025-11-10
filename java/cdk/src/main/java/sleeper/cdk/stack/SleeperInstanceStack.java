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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.stack.core.PropertiesStack;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.List;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks. Does not create Sleeper tables. If the
 * configuration for tables is provided then the optional DashboardStack will create individual dashboards for each
 * table.
 */
public class SleeperInstanceStack extends Stack {
    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceStack.class);

    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tableProperties;
    private final SleeperJarsInBucket jars;

    public SleeperInstanceStack(App app, String id, StackProps props, DeployInstanceConfiguration configuration, SleeperJarsInBucket jars) {
        super(app, id, props);
        this.instanceProperties = configuration.getInstanceProperties();
        this.tableProperties = configuration.getTableProperties();
        this.jars = jars;
    }

    public void create() {
        SleeperCoreStacks coreStacks = SleeperCoreStacks.create(this, instanceProperties, jars);
        SleeperOptionalStacks.create(this, instanceProperties, tableProperties, jars, coreStacks);
        // Only create roles after we know which policies are deployed in the instance
        coreStacks.createRoles();
        // Only write properties after CDK-defined properties are set
        new PropertiesStack(this, "Properties", instanceProperties, jars, coreStacks);
    }

}
