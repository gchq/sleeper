/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.systemtest.drivers.cdk;

import sleeper.clients.deploy.DeployInstanceConfigurationFromTemplates;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.util.cdk.InvokeCdkForInstance;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

public class DeployNewTestInstance {

    private DeployNewTestInstance() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 5 || args.length > 7) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-template> <instance-id> <vpc> <subnet> " +
                    "<optional-deploy-paused-flag> <optional-split-points-file>");
        }
        Path scriptsDir = Path.of(args[0]);
        DeployNewInstance.builder().scriptsDirectory(scriptsDir)
                .deployInstanceConfiguration(DeployInstanceConfigurationFromTemplates.builder()
                        .instancePropertiesPath(Path.of(args[1]))
                        .templatesDir(scriptsDir.resolve("templates"))
                        .tableNameForTemplate("system-test")
                        .build().load())
                .extraInstanceProperties(properties ->
                        properties.set(SYSTEM_TEST_REPO, args[2] + "/system-test"))
                .instanceId(args[2])
                .vpcId(args[3])
                .subnetIds(args[4])
                .deployPaused("true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false")))
                .splitPointsFile(optionalArgument(args, 6).map(Path::of).orElse(null))
                .instanceType(InvokeCdkForInstance.Type.SYSTEM_TEST)
                .deployWithDefaultClients();
    }
}
