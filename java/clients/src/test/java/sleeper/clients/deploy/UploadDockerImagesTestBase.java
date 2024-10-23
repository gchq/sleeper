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
package sleeper.clients.deploy;

import org.junit.jupiter.api.BeforeEach;

import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.clients.testutil.RunCommandTestHelper;
import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.InMemoryEcrRepositories;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.OptionalStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;
import static sleeper.clients.deploy.StackDockerImage.emrServerlessImage;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public abstract class UploadDockerImagesTestBase {
    private static final Map<OptionalStack, StackDockerImage> STACK_DOCKER_IMAGES = Map.of(
            OptionalStack.IngestStack, dockerBuildImage("ingest"),
            OptionalStack.EksBulkImportStack, dockerBuildImage("bulk-import-runner"),
            OptionalStack.CompactionStack, dockerBuildxImage("buildx"),
            OptionalStack.EmrServerlessBulkImportStack, emrServerlessImage("bulk-import-runner-emr-serverless"));
    private static final List<LambdaJar> LAMBDA_JARS = List.of(
            LambdaJar.builder().filenameFormat("statestore.jar").imageName("statestore-lambda").core().build(),
            LambdaJar.builder().filenameFormat("ingest.jar").imageName("ingest-task-creator-lambda").optionalStack(OptionalStack.IngestStack).build(),
            LambdaJar.builder().filenameFormat("bulk-import-starter.jar").imageName("bulk-import-starter-lambda")
                    .optionalStacks(List.of(OptionalStack.EksBulkImportStack, OptionalStack.EmrServerlessBulkImportStack)).build());
    protected final InMemoryEcrRepositories ecrClient = new InMemoryEcrRepositories();
    protected final InstanceProperties properties = createTestInstanceProperties();

    @BeforeEach
    void setUpBase() {
        properties.set(ID, "test-instance");
        properties.set(ACCOUNT, "123");
        properties.set(REGION, "test-region");
        properties.set(VERSION, "1.0.0");
    }

    protected RunCommandTestHelper.PipelineInvoker uploadEcs(InstanceProperties properties) {
        return runCommand -> uploader().upload(runCommand, UploadDockerImagesRequest.forNewDeployment(properties, ecsImageConfig()));
    }

    protected RunCommandTestHelper.PipelineInvoker uploadLambdas(InstanceProperties properties) {
        return runCommand -> uploader().upload(runCommand, UploadDockerImagesRequest.forNewDeployment(properties, lambdaImageConfig()));
    }

    protected RunCommandTestHelper.PipelineInvoker uploadLambdasForUpdate(InstanceProperties before, InstanceProperties after) {
        return runCommand -> uploader().upload(runCommand,
                UploadDockerImagesRequest.forUpdateIfNeeded(after, new PropertiesDiff(before, after), lambdaImageConfig()).orElseThrow());
    }

    protected DockerImageConfiguration ecsImageConfig() {
        return new DockerImageConfiguration(STACK_DOCKER_IMAGES, List.of());
    }

    protected DockerImageConfiguration lambdaImageConfig() {
        return new DockerImageConfiguration(Map.of(), LAMBDA_JARS);
    }

    protected abstract UploadDockerImages uploader();

    protected CommandPipeline loginDockerCommand() {
        return pipeline(command("aws", "ecr", "get-login-password", "--region", "test-region"),
                command("docker", "login", "--username", "AWS", "--password-stdin",
                        "123.dkr.ecr.test-region.amazonaws.com"));
    }

    protected CommandPipeline buildImageCommand(String tag, String dockerDirectory) {
        return pipeline(command("docker", "build", "-t", tag, dockerDirectory));
    }

    protected CommandPipeline buildImageCommandWithArgs(String... args) {
        List<String> fullArgs = new ArrayList<>(List.of("docker", "build"));
        fullArgs.addAll(List.of(args));
        return pipeline(command(fullArgs.toArray(String[]::new)));
    }

    protected CommandPipeline pushImageCommand(String tag) {
        return pipeline(command("docker", "push", tag));
    }

    protected CommandPipeline removeOldBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "rm", "sleeper"));
    }

    protected CommandPipeline createNewBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "create", "--name", "sleeper", "--use"));
    }

    protected CommandPipeline buildAndPushImageWithBuildxCommand(String tag, String dockerDirectory) {
        return pipeline(command("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64",
                "-t", tag, "--push", dockerDirectory));
    }

    protected List<CommandPipeline> commandsToLoginDockerAndPushImages(String... images) {
        List<CommandPipeline> commands = new ArrayList<>();
        commands.add(loginDockerCommand());
        for (String image : images) {
            String tag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/" + image + ":1.0.0";
            commands.add(buildImageCommand(tag, "./docker/" + image));
            commands.add(pushImageCommand(tag));
        }
        return commands;
    }
}
