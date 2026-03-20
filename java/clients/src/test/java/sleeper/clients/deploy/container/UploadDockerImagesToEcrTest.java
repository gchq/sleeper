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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.util.command.CommandFailedException;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.model.StateStoreCommitterPlatform;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildAndPushMultiplatformImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildLambdaImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.commandsToLoginDockerAndPushImages;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.createNewBuildxBuilderInstanceCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.dockerLoginToEcrCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.pullImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.pushImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.removeOldBuildxBuilderInstanceCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.tagImageCommand;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_PLATFORM;

public class UploadDockerImagesToEcrTest extends UploadDockerImagesToEcrTestBase {

    protected final Map<Path, String> files = new HashMap<>();
    DeployConfiguration deployConfig = DeployConfiguration.fromLocalBuild();

    @Nested
    @DisplayName("Upload ECS images")
    class UploadEcsImages {

        @Test
        void shouldPushImageForIngestStack() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));
        }

        @Test
        void shouldPushImagesForTwoStacks() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.EksBulkImportStack));

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildImageCommand(expectedTag2, "./docker/bulk-import-runner"),
                    pushImageCommand(expectedTag2));
        }

        @Test
        void shouldPushImageWhenEcrRepositoryPrefixIsSet() throws Exception {
            // Given
            properties.set(ECR_REPOSITORY_PREFIX, "custom-ecr-prefix");
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/custom-ecr-prefix/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));
        }
    }

    @Nested
    @DisplayName("Upload lambda images")
    class UploadLambdaImages {

        @Test
        void shouldPushCoreImage() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");

            // When
            uploadForDeployment(lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildLambdaImageCommand(expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/statestore.jar"), "statestore-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "statestore-jar-content"));
        }

        @Test
        void shouldPushImageForCoreAndOptionalLambdaInNewInstance() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");
            files.put(Path.of("./jars/ingest.jar"), "ingest-jar-content");

            // When
            uploadForDeployment(lambdaImageConfig());

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-lambda:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest-task-creator-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildLambdaImageCommand(expectedTag1, "./docker/lambda"),
                    pushImageCommand(expectedTag1),
                    buildLambdaImageCommand(expectedTag2, "./docker/lambda"),
                    pushImageCommand(expectedTag2));
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/statestore.jar"), "statestore-jar-content",
                    Path.of("./jars/ingest.jar"), "ingest-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "ingest-jar-content"));
        }

        @Test
        void shouldPushImageForOptionalLambdaWhenSeveralOfItsStacksAreEnabled() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.EmrServerlessBulkImportStack, OptionalStack.EksBulkImportStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");

            // When
            uploadForDeployment(optionalLambdasImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-starter-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildLambdaImageCommand(expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "bulk-import-starter-jar-content"));
        }

        @Test
        void shouldDoNothingWhenDeployingLambdasByJar() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.JAR);

            // When
            uploadForDeployment(lambdaImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
        }

        @Test
        void shouldDeployLambdaByDockerWhenConfiguredToAlwaysDeployByDocker() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.AthenaStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.JAR);
            files.put(Path.of("./jars/athena.jar"), "athena-jar-content");

            // When
            uploadForDeployment(lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/athena-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildLambdaImageCommand(expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/athena.jar"), "athena-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "athena-jar-content"));
        }
    }

    @Nested
    @DisplayName("Handle stacks not needing uploads")
    class HandleStacksNotNeedingUploads {
        @Test
        void shouldDoNothingWhenStackHasNoDockerImage() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.QueryStack);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
        }

        @Test
        void shouldPushImageWhenPreviousStackHasNoDockerImage() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.QueryStack, OptionalStack.IngestStack));

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            assertThat(commandsThatRan)
                    .containsExactlyElementsOf(commandsToLoginDockerAndPushImages("ingest"));
        }
    }

    @Nested
    @DisplayName("Build Docker images with buildx")
    class BuildImagesWithBuildx {

        @Test
        void shouldPushImageWhenCompactionImageNeedsToBeBuiltByBuildx() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/compaction:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildAndPushMultiplatformImageCommand(expectedTag, "./docker/compaction"));
        }

        @Test
        void shouldPushImagesWhenOnlyOneImageNeedsToBeBuiltByBuildx() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.CompactionStack));

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/compaction:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildAndPushMultiplatformImageCommand(expectedTag2, "./docker/compaction"));
        }
    }

    @Nested
    @DisplayName("Handle command failures")
    class HandleCommandFailures {

        @Test
        void shouldFailWhenDockerLoginFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            setReturnExitCodeForAllCommands(123);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(dockerLoginToEcrCommand());
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(dockerLoginToEcrCommand());
        }

        @Test
        void shouldNotFailWhenRemoveBuildxBuilderFailsForCompactionImage() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);
            setReturnExitCodeForCommand(123, removeOldBuildxBuilderInstanceCommand());

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/compaction:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand(),
                    buildAndPushMultiplatformImageCommand(expectedTag, "./docker/compaction"));
        }

        @Test
        void shouldFailWhenCreateBuildxBuilderFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);
            setReturnExitCodeForCommand(123, createNewBuildxBuilderInstanceCommand());

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(dockerDeploymentImageConfig()))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(createNewBuildxBuilderInstanceCommand());
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    removeOldBuildxBuilderInstanceCommand(),
                    createNewBuildxBuilderInstanceCommand());
        }

        @Test
        void shouldFailWhenDockerBuildFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            CommandPipeline buildImageCommand = buildImageCommand(
                    "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0",
                    "./docker/ingest");
            setReturnExitCodeForCommand(42, buildImageCommand);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(buildImageCommand);
                        assertThat(e.getExitCode()).isEqualTo(42);
                    });
            assertThat(commandsThatRan).containsExactly(dockerLoginToEcrCommand(), buildImageCommand);
        }

        @Test
        void shouldFailWhenDockerPullFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            deployConfig = DeployConfiguration.fromDockerRepository("www.test-repo.com/prefix");
            CommandPipeline pullCommand = pullImageCommand("www.test-repo.com/prefix/ingest:1.0.0");
            setReturnExitCodeForCommand(123, pullCommand);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(pullCommand);
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(dockerLoginToEcrCommand(), pullCommand);
        }

        @Test
        void shouldFailWhenDockerTagFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            deployConfig = DeployConfiguration.fromDockerRepository("www.test-repo.com/prefix");
            String sourceTag = "www.test-repo.com/prefix/ingest:1.0.0";
            String ecrTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            CommandPipeline tagCommand = tagImageCommand(sourceTag, ecrTag);
            setReturnExitCodeForCommand(123, tagCommand);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(tagCommand);
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    pullImageCommand(sourceTag),
                    tagCommand);
        }

        @Test
        void shouldFailWhenDockerPushFailsAfterPull() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            deployConfig = DeployConfiguration.fromDockerRepository("www.test-repo.com/prefix");
            String sourceTag = "www.test-repo.com/prefix/ingest:1.0.0";
            String ecrTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            CommandPipeline pushCommand = pushImageCommand(ecrTag);
            setReturnExitCodeForCommand(123, pushCommand);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(pushCommand);
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    pullImageCommand(sourceTag),
                    tagImageCommand(sourceTag, ecrTag),
                    pushCommand);
        }

        @Test
        void shouldFailWhenDockerPushFailsAfterBuild() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            String ecrTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            CommandPipeline pushCommand = pushImageCommand(ecrTag);
            setReturnExitCodeForCommand(123, pushCommand);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(pushCommand);
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(ecrTag, "./docker/ingest"),
                    pushCommand);
        }
    }

    @Nested
    @DisplayName("Handle existing images")
    class HandleExistingImages {
        @Test
        void shouldBuildAndPushImageIfImageWithDifferentVersionExists() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.addVersionToRepository("test-instance/ingest", "0.9.0");

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));
        }

        @Test
        void shouldNotBuildAndPushImageIfImageWithMatchingVersionExists() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.addVersionToRepository("test-instance/ingest", "1.0.0");

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
        }

        @Test
        void shouldForceUploadWhenVersionAlreadyExists() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.addVersionToRepository("test-instance/ingest", "1.0.0");

            // When
            uploader().upload(UploadDockerImagesToEcrRequest.forDeployment(properties, dockerDeploymentImageConfig())
                    .toBuilder().overwriteExistingTag(true).build());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(expectedTag, "./docker/ingest"),
                    pushImageCommand(expectedTag));
        }
    }

    @Nested
    @DisplayName("Pull images from repository")
    class PullImages {

        @BeforeEach
        void setUp() {
            deployConfig = DeployConfiguration.fromDockerRepository("www.test-repo.com/prefix");
        }

        @Test
        void shouldPullImageFromRepository() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String sourceTag = "www.test-repo.com/prefix/ingest:1.0.0";
            String ecrTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    pullImageCommand(sourceTag),
                    tagImageCommand(sourceTag, ecrTag),
                    pushImageCommand(ecrTag));
        }

        @Test
        void shouldNotCreateBuilderForMultiplatformImage() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String sourceTag = "www.test-repo.com/prefix/compaction:1.0.0";
            String ecrTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/compaction:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    pullImageCommand(sourceTag),
                    tagImageCommand(sourceTag, ecrTag),
                    pushImageCommand(ecrTag));
        }

        @Test
        void shouldNotCopyFileForLambdaImage() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");

            // When
            uploadForDeployment(lambdaImageConfig());

            // Then
            String sourceTag = "www.test-repo.com/prefix/statestore-lambda:1.0.0";
            String ecrTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    pullImageCommand(sourceTag),
                    tagImageCommand(sourceTag, ecrTag),
                    pushImageCommand(ecrTag));
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/statestore.jar"), "statestore-jar-content"));
        }
    }

    @Nested
    @DisplayName("State store committer image")
    class StateStoreCommitterImage {

        @BeforeEach
        void setUp() {
            properties.setList(OPTIONAL_STACKS, List.of());
        }

        @Test
        void shouldPushStateStoreCommitterImageForEc2Platform() throws Exception {
            // Given
            properties.setEnum(STATESTORE_COMMITTER_PLATFORM, StateStoreCommitterPlatform.EC2);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-committer:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    dockerLoginToEcrCommand(),
                    buildImageCommand(expectedTag, "./docker/statestore-committer"),
                    pushImageCommand(expectedTag));
        }

        @Test
        void shouldNotPushStateStoreCommitterImageForLambdaPlatform() throws Exception {
            // Given
            properties.setEnum(STATESTORE_COMMITTER_PLATFORM, StateStoreCommitterPlatform.LAMBDA);

            // When
            uploadForDeployment(dockerDeploymentImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
        }
    }

    @Override
    protected UploadDockerImagesToEcr uploader() {
        return new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .deployConfig(deployConfig)
                        .commandRunner(commandRunner)
                        .copyFile((source, target) -> files.put(target, files.get(source)))
                        .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                        .version("1.0.0")
                        .build(),
                ecrClient, "123", "test-region");
    }
}
