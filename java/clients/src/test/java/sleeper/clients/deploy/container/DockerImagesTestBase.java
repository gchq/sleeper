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

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.model.OptionalStack;

import java.util.ArrayList;
import java.util.List;

import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCode;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCodeForCommand;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class DockerImagesTestBase {
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
            LambdaHandler.builder()
                    .jar(LambdaJar.builder()
                            .filenameFormat("statestore.jar")
                            .imageName("statestore-lambda")
                            .artifactID("statestore-lambda").build())
                    .handler("StateStoreCommitterLambda").core().build(),
            LambdaHandler.builder()
                    .jar(LambdaJar.builder()
                            .filenameFormat("ingest.jar")
                            .imageName("ingest-task-creator-lambda")
                            .artifactID("ingest-task-creator-lambda").build())
                    .handler("IngestTaskCreatorLambda")
                    .optionalStack(OptionalStack.IngestStack).build(),
            LambdaHandler.builder()
                    .jar(LambdaJar.builder()
                            .filenameFormat("bulk-import-starter.jar")
                            .imageName("bulk-import-starter-lambda")
                            .artifactID("bulk-import-starter-lambda").build())
                    .handler("BulkImportStarterLambda")
                    .optionalStacks(List.of(OptionalStack.EksBulkImportStack, OptionalStack.EmrServerlessBulkImportStack)).build(),
            LambdaHandler.builder()
                    .jar(LambdaJar.builder()
                            .filenameFormat("athena.jar")
                            .imageName("athena-lambda")
                            .artifactID("athena-lambda")
                            .alwaysDockerDeploy(true).build())
                    .handler("AthenaLambda")
                    .optionalStacks(List.of(OptionalStack.AthenaStack)).build());

    protected final List<CommandPipeline> commandsThatRan = new ArrayList<>();
    protected CommandPipelineRunner commandRunner = recordCommandsRun(commandsThatRan);

    protected void setReturnExitCodeForAllCommands(int exitCode) {
        commandRunner = recordCommandsRun(commandsThatRan, returnExitCode(exitCode));
    }

    protected void setReturnExitCodeForCommand(int exitCode, CommandPipeline command) {
        commandRunner = recordCommandsRun(commandsThatRan, returnExitCodeForCommand(exitCode, command));
    }

    protected DockerImageConfiguration dockerDeploymentImageConfig() {
        return new DockerImageConfiguration(DOCKER_DEPLOYMENTS, List.of());
    }

    protected DockerImageConfiguration lambdaImageConfig() {
        return new DockerImageConfiguration(List.of(), LAMBDA_HANDLERS);
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

    protected CommandPipeline buildAndPushMultiplatformImageCommand(String tag, String dockerDirectory) {
        return pipeline(command("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64",
                "-t", tag, "--push", dockerDirectory));
    }

}
