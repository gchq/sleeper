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

import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.EcrRepositoriesInMemory;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.DockerImageConfiguration.from;
import static sleeper.clients.testutil.RunCommandTestHelper.command;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;
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
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(pipelinesThatRan).containsExactly(
                    loginDockerPipeline(),
                    buildImagePipeline(expectedTag, "./docker/ingest"),
                    pushImagePipeline(expectedTag));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }

        @Test
        void shouldCreateRepositoriesAndPushImagesForTwoStacks() throws IOException, InterruptedException {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack,EksBulkImportStack");

            // When
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner:1.0.0";
            assertThat(pipelinesThatRan).containsExactly(
                    loginDockerPipeline(),
                    buildImagePipeline(expectedTag1, "./docker/ingest"),
                    pushImagePipeline(expectedTag1),
                    buildImagePipeline(expectedTag2, "./docker/bulk-import-runner"),
                    pushImagePipeline(expectedTag2));

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
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            assertThat(pipelinesThatRan).isEmpty();

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }

        @Test
        void shouldDoNothingWhenStackHasNoDockerImage() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "OtherStack");

            // When
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            assertThat(pipelinesThatRan).isEmpty();
            assertThat(ecrClient.getCreatedRepositories()).isEmpty();
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenPreviousStackHasNoDockerImage() throws Exception {
            // Given
            properties.set(OPTIONAL_STACKS, "OtherStack,IngestStack");

            // When
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            assertThat(pipelinesThatRan)
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
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(pipelinesThatRan).containsExactly(
                    loginDockerPipeline(),
                    removeOldBuildXBuilderInstancePipeline(),
                    createNewBuildXBuilderInstancePipeline(),
                    buildAndPushImageWithBuildxPipeline(expectedTag, "./docker/buildx"));

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx");
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenOnlyOneImageNeedsToBeBuiltByBuildx() throws IOException, InterruptedException {
            // Given
            properties.set(OPTIONAL_STACKS, "IngestStack,BuildxStack");

            // When
            List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/buildx:1.0.0";
            assertThat(pipelinesThatRan).containsExactly(
                    loginDockerPipeline(),
                    removeOldBuildXBuilderInstancePipeline(),
                    createNewBuildXBuilderInstancePipeline(),
                    buildImagePipeline(expectedTag1, "./docker/ingest"),
                    pushImagePipeline(expectedTag1),
                    buildAndPushImageWithBuildxPipeline(expectedTag2, "./docker/buildx")
            );

            assertThat(ecrClient.getCreatedRepositories())
                    .containsExactlyInAnyOrder("test-instance/buildx", "test-instance/ingest");
        }
    }

    private CommandPipeline loginDockerPipeline() {
        return pipeline(command("aws", "ecr", "get-login-password", "--region", "test-region"),
                command("docker", "login", "--username", "AWS", "--password-stdin",
                        "123.dkr.ecr.test-region.amazonaws.com"));
    }

    private CommandPipeline buildImagePipeline(String tag, String dockerDirectory) {
        return pipeline(command("docker", "build", "-t", tag, dockerDirectory));
    }

    private CommandPipeline pushImagePipeline(String tag) {
        return pipeline(command("docker", "push", tag));
    }

    private CommandPipeline removeOldBuildXBuilderInstancePipeline() {
        return pipeline(command("docker", "buildx", "rm", "sleeper", "||", "true"));
    }

    private CommandPipeline createNewBuildXBuilderInstancePipeline() {
        return pipeline(command("docker", "buildx", "create", "--name", "sleeper", "--use"));
    }

    private CommandPipeline buildAndPushImageWithBuildxPipeline(String tag, String dockerDirectory) {
        return pipeline(command("docker", "buildx", "build", "--platform", "linux/amd64,linux/arm64",
                "-t", tag, "--push", dockerDirectory));
    }

    private List<CommandPipeline> commandsToLoginDockerAndPushImages(String... images) {
        List<CommandPipeline> pipelines = new ArrayList<>();
        pipelines.add(loginDockerPipeline());
        for (String image : images) {
            String tag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/" + image + ":1.0.0";
            pipelines.add(buildImagePipeline(tag, "./docker/" + image));
            pipelines.add(pushImagePipeline(tag));
        }
        return pipelines;
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
