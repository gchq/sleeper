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
package sleeper.clients.images;

import sleeper.clients.util.command.CommandUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static sleeper.clients.util.command.Command.envAndCommand;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public abstract class DockerImageTestBase extends LocalStackTestBase {

    protected InstanceProperties instanceProperties = createTestInstanceProperties();

    protected void runDockerImage(String dockerImage, String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.addAll(List.of("docker", "run", "--rm", "-it", "--network=host"));

        Map<String, String> environment = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        environment.put("AWS_ENDPOINT_URL", localStackContainer.getEndpoint().toString());
        environment.keySet().forEach(variable -> command.addAll(List.of("--env", variable)));
        environment.putAll(System.getenv());

        command.add(dockerImage);
        command.addAll(List.of(arguments));

        CommandUtils.runCommandLogOutputWithPty(envAndCommand(environment, command.toArray(String[]::new)));
    }

}
