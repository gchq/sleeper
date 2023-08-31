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

package sleeper.clients.deploy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.CommandFailedException;
import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.EcrRepositoriesInMemory;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.deploy.DockerImageConfiguration.from;
import static sleeper.clients.testutil.RunCommandTestHelper.command;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCode;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCodeForCommand;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class UploadDockerImagesNewTest {
    private static final Map<String, String> DIRECTORY_BY_STACK = Map.of(
            "IngestStack", "ingest",
            "EksBulkImportStack", "bulk-import-runner",
            "BuildxStack", "buildx");
    private static final List<String> BUILDX_STACKS = List.of("BuildxStack");
    private final EcrRepositoriesInMemory ecrClient = new EcrRepositoriesInMemory();
    private final InstanceProperties properties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        properties.set(ID, "test-instance");
        properties.set(ACCOUNT, "123");
        properties.set(REGION, "test-region");
    }

    @Nested
    @DisplayName("Upload Docker images")
    class UploadDockerImages {

        @Test
        void shouldCreateRepositoryAndPushImageForIngestStack() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }

        @Test
        void shouldCreateRepositoriesAndPushImagesForTwoStacks() throws IOException, InterruptedException {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack,EksBulkImportStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildImageCommand(expectedTag2, "./docker/bulk-import-runner"),
                    pushImageCommand(expectedTag2));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest", "test-instance/bulk-import-runner");
        }
    }

    @Nested
    @DisplayName("Handle stacks not needing uploads")
    class HandleStacksNotNeedingUploads {

        @Test
        void shouldDoNothingWhenRepositoryAlreadyExists() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack");
            ecrClient.createRepository("test-instance/ingest");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            assertThat(commandsThatRan).isEmpty();

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }

        @Test
        void shouldDoNothingWhenStackHasNoDockerImage() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "OtherStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getCreatedRepositories()).isEmpty();
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenPreviousStackHasNoDockerImage() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "OtherStack,IngestStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            assertThat(commandsThatRan)
                    .containsExactlyElementsOf(commandsToLoginDockerAndPushImages("ingest"));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }
    }

    @Nested
    @DisplayName("Build Docker images with buildx")
    class BuildImagesWithBuildx {

        @Test
        void shouldCreateRepositoryAndPushImageWhenImageNeedsToBeBuiltByBuildx() throws IOException, InterruptedException {
            // Given
            properties.set(OPTIONAL_STACKS, "BuildxStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildAndPushImageWithBuildxCommand(expectedTag, "./docker/buildx"));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx");
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenOnlyOneImageNeedsToBeBuiltByBuildx() throws IOException, InterruptedException {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack,BuildxStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildAndPushImageWithBuildxCommand(expectedTag2, "./docker/buildx")
            );

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx", "test-instance/ingest");
        }
    }

    @Nested
    @DisplayName("Handle command failures")
    class HandleCommandFailures {

        @Test
        void shouldFailWhenDockerLoginFails() {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack");

            // When / Then
            assertThatThrownBy(() ->
                    getUpload().upload(returningExitCode(123))
            ).isInstanceOfSatisfying(CommandFailedException.class, e -> {
                assertThat(e.getCommand()).isEqualTo(loginDockerCommand());
                assertThat(e.getExitCode()).isEqualTo(123);
            });
            assertThat(ecrClient.getCreatedRepositories()).isEmpty();
        }

        @Test
        void shouldNotFailWhenRemoveBuildxBuilderFails() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "BuildxStack");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(getUpload()::upload,
                    returningExitCodeForCommand(123, removeOldBuildxBuilderInstanceCommand()));

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildAndPushImageWithBuildxCommand(expectedTag, "./docker/buildx"));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx");
        }

        @Test
        void shouldFailWhenCreateBuildxBuilderFails() {
            // Given
            properties.set(OPTIONAL_STACKS, "BuildxStack");

            // When / Then
            assertThatThrownBy(() ->
                    getUpload().upload(returningExitCodeForCommand(
                            123, createNewBuildxBuilderInstanceCommand()))
            ).isInstanceOfSatisfying(CommandFailedException.class, e -> {
                assertThat(e.getCommand()).isEqualTo(createNewBuildxBuilderInstanceCommand());
                assertThat(e.getExitCode()).isEqualTo(123);
            });
            assertThat(ecrClient.getCreatedRepositories()).isEmpty();
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

    private UploadDockerImagesNew getUpload() {
        return UploadDockerImagesNew.builder()
                .baseDockerDirectory(Path.of("./docker"))
                .version("1.0.0")
                .instanceProperties(properties)
                .ecrClient(ecrClient)
                .dockerImageConfig(from(DIRECTORY_BY_STACK, BUILDX_STACKS))
                .build();
    }
}
