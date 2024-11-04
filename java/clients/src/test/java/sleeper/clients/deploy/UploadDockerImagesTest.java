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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.CommandFailedException;
import sleeper.clients.util.CommandPipeline;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;
import sleeper.core.properties.validation.OptionalStack;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCode;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCodeForCommand;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class UploadDockerImagesTest extends UploadDockerImagesTestBase {

    protected final Map<Path, String> files = new HashMap<>();

    @Nested
    @DisplayName("Upload ECS images")
    class UploadEcsImages {

        @Test
        void shouldCreateRepositoryAndPushImageForIngestStack() throws Exception {
            // Given
            properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER.toString());
            files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadLambdas(properties));

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
            properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER.toString());
            files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");
            files.put(Path.of("./jars/ingest.jar"), "ingest-jar-content");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadLambdas(properties));

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
            properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER.toString());
            files.put(Path.of("./jars/ingest.jar"), "ingest-jar-content");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadLambdasForUpdate(propertiesBefore, properties));

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
            properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER.toString());
            files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadLambdasForUpdate(propertiesBefore, properties));

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
            properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER.toString());
            files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadLambdasForUpdate(propertiesBefore, properties));

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
            properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.JAR.toString());

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadLambdas(properties));

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories()).isEmpty();
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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories()).isEmpty();
        }

        @Test
        void shouldCreateRepositoryAndPushImageWhenPreviousStackHasNoDockerImage() throws Exception {
            // Given
            properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.AthenaStack, OptionalStack.IngestStack));

            // When
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            assertThatThrownBy(() -> uploader().upload(returningExitCode(123),
                    UploadDockerImagesRequest.forNewDeployment(properties, ecsImageConfig())))
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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties),
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
                    UploadDockerImagesRequest.forNewDeployment(properties, ecsImageConfig())))
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
                    UploadDockerImagesRequest.forNewDeployment(properties, ecsImageConfig())))
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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

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
            List<CommandPipeline> commandsThatRan = pipelinesRunOn(uploadEcs(properties));

            // Then
            assertThat(commandsThatRan).isEmpty();
            assertThat(ecrClient.getRepositories())
                    .containsExactlyInAnyOrder("test-instance/ingest");
        }
    }

    @Override
    protected UploadDockerImages uploader() {
        return UploadDockerImages.builder()
                .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                .ecrClient(ecrClient)
                .copyFile((source, target) -> files.put(target, files.get(source)))
                .build();
    }
}
