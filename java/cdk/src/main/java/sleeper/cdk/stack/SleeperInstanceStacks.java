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

import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.stack.core.PropertiesStack;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks. Does not create Sleeper tables. If the
 * configuration for tables is provided then the optional DashboardStack will create individual dashboards for each
 * table.
 */
public class SleeperInstanceStacks {

    private SleeperInstanceStacks() {
    }

    /**
     * Adds an instance of Sleeper to the CDK app.
     *
     * @param scope         the scope to add the Sleeper instance to
     * @param configuration the configuration of the instance
     * @param s3Client      the S3 client to use to scan the jars bucket for jars to deploy
     */
    public static void create(Construct scope, DeployInstanceConfiguration configuration, S3Client s3Client) {
        SleeperJarsInBucket jars = SleeperJarsInBucket.from(s3Client, configuration.getInstanceProperties());
        create(scope, configuration, jars);
    }

    /**
     * Adds an instance of Sleeper to the CDK app.
     *
     * @param scope         the scope to add the Sleeper instance to
     * @param configuration the configuration of the instance
     * @param jars          a pointer to the jars in S3 to use to deploy lambdas
     */
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
