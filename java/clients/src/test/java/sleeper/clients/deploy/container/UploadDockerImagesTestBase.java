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
import sleeper.clients.testutil.RunCommandTestHelper;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;

import java.util.ArrayList;
import java.util.List;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public abstract class UploadDockerImagesTestBase {
    private static final List<DockerDeployment> DOCKER_DEPLOYMENTS = List.of(
            DockerDeployment.builder()
                    .deploymentName("ingest")
                    .optionalStack(OptionalStack.IngestStack)
                    .build(),
            DockerDeployment.builder()
                    .deploymentName("bulk-import-runner")
                    .optionalStack(OptionalStack.EksBulkImportStack)
                    .build(),
            DockerDeployment.builder()
                    .deploymentName("compaction")
                    .optionalStack(OptionalStack.CompactionStack)
                    .multiplatform(true)
                    .build(),
            DockerDeployment.builder()
                    .deploymentName("bulk-import-runner-emr-serverless")
                    .optionalStack(OptionalStack.EmrServerlessBulkImportStack)
                    .createEmrServerlessPolicy(true)
                    .build());
    private static final List<LambdaHandler> LAMBDA_HANDLERS = List.of(
            LambdaHandler.builder().jar(LambdaJar.withFormatAndImage("statestore.jar", "statestore-lambda"))
                    .handler("StateStoreCommitterLambda").core().build(),
            LambdaHandler.builder().jar(LambdaJar.withFormatAndImage("ingest.jar", "ingest-task-creator-lambda"))
                    .handler("IngestTaskCreatorLambda")
                    .optionalStack(OptionalStack.IngestStack).build(),
            LambdaHandler.builder().jar(LambdaJar.withFormatAndImage("bulk-import-starter.jar", "bulk-import-starter-lambda"))
                    .handler("BulkImportStarterLambda")
                    .optionalStacks(List.of(OptionalStack.EksBulkImportStack, OptionalStack.EmrServerlessBulkImportStack)).build(),
            LambdaHandler.builder().jar(LambdaJar.withFormatAndImageDeployWithDocker("athena.jar", "athena-lambda"))
                    .handler("AthenaLambda")
                    .optionalStacks(List.of(OptionalStack.AthenaStack)).build());
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
        return new DockerImageConfiguration(DOCKER_DEPLOYMENTS, List.of());
    }

    protected DockerImageConfiguration lambdaImageConfig() {
        return new DockerImageConfiguration(List.of(), LAMBDA_HANDLERS);
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
