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
package sleeper.clients.deploy.container;

import org.junit.jupiter.api.BeforeEach;

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public abstract class UploadDockerImagesToEcrTestBase extends DockerImagesTestBase {
    protected final InMemoryEcrRepositories ecrClient = new InMemoryEcrRepositories();
    protected final InstanceProperties properties = createTestInstanceProperties();

    @BeforeEach
    void setUpBase() {
        properties.set(ID, "test-instance");
        properties.set(VERSION, "1.0.0");
    }

    protected void uploadForDeployment(DockerImageConfiguration imageConfig) throws Exception {
        uploader().upload(UploadDockerImagesToEcrRequest.forDeployment(properties, imageConfig));
    }

    protected void uploadForUpdate(InstanceProperties before, InstanceProperties after, DockerImageConfiguration imageConfig) throws Exception {
        uploader().upload(UploadDockerImagesToEcrRequest.forUpdateIfNeeded(after, new PropertiesDiff(before, after), imageConfig).orElseThrow());
    }

    protected abstract UploadDockerImagesToEcr uploader();

    protected CommandPipeline dockerLoginToEcrCommand() {
        return pipeline(command("aws", "ecr", "get-login-password", "--region", "test-region"),
                command("docker", "login", "--username", "AWS", "--password-stdin",
                        "123.dkr.ecr.test-region.amazonaws.com"));
    }

    protected List<CommandPipeline> commandsToLoginDockerAndPushImages(String... images) {
        List<CommandPipeline> commands = new ArrayList<>();
        commands.add(dockerLoginToEcrCommand());
        for (String image : images) {
            String tag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/" + image + ":1.0.0";
            commands.add(buildImageCommand(tag, "./docker/" + image));
            commands.add(pushImageCommand(tag));
        }
        return commands;
    }
}
