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
package sleeper.systemtest.cdk;

import sleeper.clients.deploy.CdkDeployInstance;
import sleeper.clients.deploy.DeployNewInstance;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_REPO;

public class DeployNewTestInstance {

    private DeployNewTestInstance() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (5 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <properties-template> <instance-id> <vpc> <subnet>");
        }
        DeployNewInstance.builder().scriptsDirectory(Path.of(args[0]))
                .instancePropertiesTemplate(Path.of(args[1]))
                .extraInstanceProperties(properties ->
                        properties.setProperty(SYSTEM_TEST_REPO.getPropertyName(), args[2] + "/system-test"))
                .instanceId(args[2])
                .vpcId(args[3])
                .subnetId(args[4])
                .tableName("system-test")
                .instanceType(CdkDeployInstance.Type.SYSTEM_TEST)
                .deployWithDefaultClients();
    }
}
