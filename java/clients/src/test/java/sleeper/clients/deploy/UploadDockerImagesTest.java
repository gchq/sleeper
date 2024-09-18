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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.RunCommandTestHelper;
import sleeper.clients.util.CommandFailedException;
import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.InMemoryEcrRepositories;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.validation.OptionalStack;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;
import static sleeper.clients.deploy.StackDockerImage.emrServerlessImage;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCode;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCodeForCommand;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class UploadDockerImagesTest {
    private static final Map<OptionalStack, StackDockerImage> STACK_DOCKER_IMAGES = Map.of(
            OptionalStack.IngestStack, dockerBuildImage("ingest"),
            OptionalStack.EksBulkImportStack, dockerBuildImage("bulk-import-runner"),
            OptionalStack.CompactionStack, dockerBuildxImage("buildx"),
            OptionalStack.EmrServerlessBulkImportStack, emrServerlessImage("bulk-import-runner-emr-serverless"));
    private final InMemoryEcrRepositories ecrClient = new InMemoryEcrRepositories();
    private final InstanceProperties properties = createTestInstanceProperties();
    private final DockerImageConfiguration dockerImageConfiguration = new DockerImageConfiguration(STACK_DOCKER_IMAGES);

    @BeforeEach
    void setUp() {
        properties.set(ID, "test-instance");
        properties.set(ACCOUNT, "123");
        properties.set(REGION, "test-region");
        properties.set(VERSION, "1.0.0");
    }

    private UploadDockerImages uploader() {
        return UploadDockerImages.builder()
                .baseDockerDirectory(Path.of("./docker"))
                .ecrClient(ecrClient)
                .dockerImageConfig(dockerImageConfiguration)
                .build();
    }

    private RunCommandTestHelper.PipelineInvoker upload(InstanceProperties properties) {
        return runCommand -> uploader().upload(runCommand, StacksForDockerUpload.from(properties));
    }

    @Nested
    @DisplayName("Upload images")
    class UploadImages {

        @Test
        void shouldCreateRepositoryAndPushImageForIngestStack() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }

        @Test
        void shouldCreateRepositoriesAndPushImagesForTwoStacks() throws IOException, InterruptedException {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.EksBulkImportStack));

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildImageCommand(expectedTag2, "./docker/bulk-import-runner"),
                    pushImageCommand(expectedTag2));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest", "test-instance/bulk-import-runner");
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenEcrRepositoryPrefixIsSet() throws Exception {
            // Given
            properties.set(ECR_REPOSITORY_PREFIX, "custom-ecr-prefix");
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/custom-ecr-prefix/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));
            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("custom-ecr-prefix/ingest");
        }
    }

    @Nested
    @DisplayName("Handle stacks not needing uploads")
    class HandleStacksNotNeedingUploads {
        @Test
        void shouldDoNothingWhenStackHasNoDockerImage() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.AthenaStack);

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenPreviousStackHasNoDockerImage() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.AthenaStack, OptionalStack.IngestStack));

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            assertThat(commandsThatRan)
                    .containsExactlyElementsOf(commandsToLoginDockerAndPushImages("ingest"));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }
    }

    @Nested
    @DisplayName("Build Docker images with buildx")
    class BuildImagesWithBuildx {

        @Test
        void shouldCreateRepositoryAndPushImageWhenImageNeedsToBeBuiltByBuildx() throws IOException, InterruptedException {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildAndPushImageWithBuildxCommand(expectedTag, "./docker/buildx"));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx");
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenOnlyOneImageNeedsToBeBuiltByBuildx() throws IOException, InterruptedException {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.CompactionStack));

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildAndPushImageWithBuildxCommand(expectedTag2, "./docker/buildx"));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx", "test-instance/ingest");
        }
    }

    @Nested
    @DisplayName("Create EMR Serverless security policy")
    class CreateEMRServerlessSecurityPolicy {

        @Test
        void shouldCreateSecurityPolicyToGiveAccessToEmrServerlessImageOnly() throws IOException, InterruptedException {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.EmrServerlessBulkImportStack));

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner-emr-serverless:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildImageCommand(expectedTag2, "./docker/bulk-import-runner-emr-serverless"),
                    pushImageCommand(expectedTag2));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest", "test-instance/bulk-import-runner-emr-serverless");
            assertThat(ecrClient.getRepositoriesWithEmrServerlessPolicy())
                    .containsExactly("test-instance/bulk-import-runner-emr-serverless");
        }
    }

    @Nested
    @DisplayName("Handle command failures")
    class HandleCommandFailures {

        @Test
        void shouldFailWhenDockerLoginFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When / Then
            assertThatThrownBy(() -> uploader().upload(returningExitCode(123), StacksForDockerUpload.from(properties)))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(loginDockerCommand());
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldNotFailWhenRemoveBuildxBuilderFails() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties),
                    returningExitCodeForCommand(123, removeOldBuildxBuilderInstanceCommand()));

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildAndPushImageWithBuildxCommand(expectedTag, "./docker/buildx"));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx");
        }

        @Test
        void shouldFailWhenCreateBuildxBuilderFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);

            // When / Then
            assertThatThrownBy(() -> uploader().upload(
                    returningExitCodeForCommand(123, createNewBuildxBuilderInstanceCommand()),
                    StacksForDockerUpload.from(properties)))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(createNewBuildxBuilderInstanceCommand());
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldDeleteRepositoryWhenDockerBuildFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When / Then
            CommandPipeline buildImageCommand = buildImageCommand(
                    "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0",
                    "./docker/ingest");
            assertThatThrownBy(() -> uploader().upload(
                    returningExitCodeForCommand(42, buildImageCommand),
                    StacksForDockerUpload.from(properties)))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(buildImageCommand);
                        assertThat(e.getExitCode()).isEqualTo(42);
                    });
            assertThat(ecrClient.getRepositories()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Handle existing images")
    class HandleExistingImages {
        @Test
        void shouldBuildAndPushImageIfImageWithDifferentVersionExists() throws IOException, InterruptedException {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.createRepository("test-instance/ingest");
            ecrClient.addVersionToRepository("test-instance/ingest", "0.9.0");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }

        @Test
        void shouldNotBuildAndPushImageIfImageWithMatchingVersionExists() throws IOException, InterruptedException {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.createRepository("test-instance/ingest");
            ecrClient.addVersionToRepository("test-instance/ingest", "1.0.0");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(upload(properties));

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }
    }

    private CommandPipeline loginDockerCommand() {
        return pipeline(command("aws", "ecr", "get-login-password", "--region", "test-region"),
                command("docker", "login", "--username", "AWS", "--password-stdin",
                        "123.dkr.ecr.test-region.amazonaws.com"));
    }

    private CommandPipeline buildImageCommand(String tag, String dockerDirectory) {
        return pipeline(command("docker", "build", "-t", tag, dockerDirectory));
    }

    private CommandPipeline pushImageCommand(String tag) {
        return pipeline(command("docker", "push", tag));
    }

    private CommandPipeline removeOldBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "rm", "sleeper"));
    }

    private CommandPipeline createNewBuildxBuilderInstanceCommand() {
        return pipeline(command("docker", "buildx", "create", "--name", "sleeper", "--use"));
    }

    private CommandPipeline buildAndPushImageWithBuildxCommand(String tag, String dockerDirectory) {
        return pipeline(command("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64",
                "-t", tag, "--push", dockerDirectory));
    }

    private List<CommandPipeline> commandsToLoginDockerAndPushImages(String... images) {
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
