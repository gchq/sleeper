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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.command.CommandFailedException;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class UploadDockerImagesToEcrTest extends UploadDockerImagesToEcrTestBase {

    protected final Map<Path, String> files = new HashMap<>();

    @Nested
    @DisplayName("Upload ECS images")
    class UploadEcsImages {

        @Test
        void shouldCreateRepositoryAndPushImageForIngestStack() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

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
        void shouldCreateRepositoriesAndPushImagesForTwoStacks() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.EksBulkImportStack));

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

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
            uploadForNewDeployment(dockerDeploymentImageConfig());

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
    @DisplayName("Upload lambda images")
    class UploadLambdaImages {

        @Test
        void shouldCreateRepositoryAndPushCoreImage() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");

            // When
            uploadForNewDeployment(lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommandWithArgs("-t", expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/statestore-lambda");
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
            uploadForNewDeployment(lambdaImageConfig());

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-lambda:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest-task-creator-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommandWithArgs("-t", expectedTag1, "./docker/lambda"),
                    pushImageCommand(expectedTag1),
                    buildImageCommandWithArgs("-t", expectedTag2, "./docker/lambda"),
                    pushImageCommand(expectedTag2));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder(
                            "test-instance/statestore-lambda",
                            "test-instance/ingest-task-creator-lambda");
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/statestore.jar"), "statestore-jar-content",
                    Path.of("./jars/ingest.jar"), "ingest-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "ingest-jar-content"));
        }

        @Test
        void shouldPushImageForOptionalLambdaWhenAdded() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            InstanceProperties propertiesBefore = InstanceProperties.copyOf(properties);
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/ingest.jar"), "ingest-jar-content");

            // When
            uploadForUpdate(propertiesBefore, properties, lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest-task-creator-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommandWithArgs("-t", expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest-task-creator-lambda");
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/ingest.jar"), "ingest-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "ingest-jar-content"));
        }

        @Test
        void shouldPushImageForOptionalLambdaWhenOneOfItsStacksIsAdded() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            InstanceProperties propertiesBefore = InstanceProperties.copyOf(properties);
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.EmrServerlessBulkImportStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");

            // When
            uploadForUpdate(propertiesBefore, properties, lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-starter-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommandWithArgs("-t", expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/bulk-import-starter-lambda");
            assertThat(files).isEqualTo(Map.of(
                    Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content",
                    Path.of("./docker/lambda/lambda.jar"), "bulk-import-starter-jar-content"));
        }

        @Test
        void shouldPushImageForOptionalLambdaWhenSeveralOfItsStacksAreAdded() throws Exception {
            // Given
            properties.setList(OPTIONAL_STACKS, List.of());
            InstanceProperties propertiesBefore = InstanceProperties.copyOf(properties);
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.EmrServerlessBulkImportStack, OptionalStack.EksBulkImportStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER);
            files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");

            // When
            uploadForUpdate(propertiesBefore, properties, lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-starter-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommandWithArgs("-t", expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/bulk-import-starter-lambda");
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
            uploadForNewDeployment(lambdaImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldDeployLambdaByDockerWhenConfiguredToAlwaysDeployByDocker() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.AthenaStack));
            properties.setEnum(LAMBDA_DEPLOY_TYPE, LambdaDeployType.JAR);
            files.put(Path.of("./jars/athena.jar"), "athena-jar-content");

            // When
            uploadForNewDeployment(lambdaImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/athena-lambda:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommandWithArgs("-t", expectedTag, "./docker/lambda"),
                    pushImageCommand(expectedTag));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/athena-lambda");
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
            uploadForNewDeployment(dockerDeploymentImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenPreviousStackHasNoDockerImage() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.QueryStack, OptionalStack.IngestStack));

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

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
        void shouldCreateRepositoryAndPushImageWhenCompactionImageNeedsToBeBuiltByBuildx() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/compaction:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildAndPushMultiplatformImageCommand(expectedTag, "./docker/compaction"));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/compaction");
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenOnlyOneImageNeedsToBeBuiltByBuildx() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.CompactionStack));

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

            // Then
            String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
            String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/compaction:1.0.0";
            assertThat(commandsThatRan).containsExactly(
                    loginDockerCommand(),
                    buildImageCommand(expectedTag1, "./docker/ingest"),
                    pushImageCommand(expectedTag1),
                    buildAndPushMultiplatformImageCommand(expectedTag2, "./docker/compaction"));

            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/compaction", "test-instance/ingest");
        }
    }

    @Nested
    @DisplayName("Create EMR Serverless security policy")
    class CreateEMRServerlessSecurityPolicy {

        @Test
        void shouldCreateSecurityPolicyToGiveAccessToEmrServerlessImageOnly() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack, OptionalStack.EmrServerlessBulkImportStack));

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

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
            setReturnExitCodeForAllCommands(123);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForNewDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(loginDockerCommand());
                        assertThat(e.getExitCode()).isEqualTo(123);
                    });
            assertThat(commandsThatRan).containsExactly(loginDockerCommand());
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldDeleteRepositoryWhenDockerBuildFails() {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            CommandPipeline buildImageCommand = buildImageCommand(
                    "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0",
                    "./docker/ingest");
            setReturnExitCodeForCommand(42, buildImageCommand);
            DockerImageConfiguration imageConfig = dockerDeploymentImageConfig();

            // When / Then
            assertThatThrownBy(() -> uploadForNewDeployment(imageConfig))
                    .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                        assertThat(e.getCommand()).isEqualTo(buildImageCommand);
                        assertThat(e.getExitCode()).isEqualTo(42);
                    });
            assertThat(commandsThatRan).containsExactly(loginDockerCommand(), buildImageCommand);
            assertThat(ecrClient.getRepositories()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Handle existing images")
    class HandleExistingImages {
        @Test
        void shouldBuildAndPushImageIfImageWithDifferentVersionExists() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.createRepository("test-instance/ingest");
            ecrClient.addVersionToRepository("test-instance/ingest", "0.9.0");

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

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
        void shouldNotBuildAndPushImageIfImageWithMatchingVersionExists() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);
            ecrClient.createRepository("test-instance/ingest");
            ecrClient.addVersionToRepository("test-instance/ingest", "1.0.0");

            // When
            uploadForNewDeployment(dockerDeploymentImageConfig());

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }
    }

    @Override
    protected UploadDockerImagesToEcr uploader() {
        return new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .commandRunner(commandRunner)
                        .copyFile((source, target) -> files.put(target, files.get(source)))
                        .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                        .version("1.0.0")
                        .build(),
                ecrClient);
    }
}
