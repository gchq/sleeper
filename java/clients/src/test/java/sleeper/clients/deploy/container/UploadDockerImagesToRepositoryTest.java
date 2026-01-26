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
import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.util.command.CommandFailedException;
import sleeper.clients.util.command.CommandPipeline;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildAndPushMultiplatformImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildLambdaImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.createNewBuildxBuilderInstanceCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.pushImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.removeOldBuildxBuilderInstanceCommand;

@DisplayName("Upload Docker images")
public class UploadDockerImagesToRepositoryTest extends DockerImagesTestBase {

    protected final Map<Path, String> files = new HashMap<>();

    @Test
    void shouldBuildAndPushDockerDeploymentImages() throws Exception {

        // Given
        DockerImageConfiguration dockerImageConfiguration = dockerDeploymentImageConfig();

        // When
        uploadAllImages(dockerImageConfiguration);

        // Then
        String expectedCommitterTag = "www.somedocker.com/prefix/statestore-committer:1.0.0";
        String expectedIngestTag = "www.somedocker.com/prefix/ingest:1.0.0";
        String expectedBulkImportTag = "www.somedocker.com/prefix/bulk-import-runner:1.0.0";
        String expectedCompactionTag = "www.somedocker.com/prefix/compaction:1.0.0";
        String expectedEmrTag = "www.somedocker.com/prefix/bulk-import-runner-emr-serverless:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                removeOldBuildxBuilderInstanceCommand(),
                createNewBuildxBuilderInstanceCommand(),
                buildImageCommand(expectedCommitterTag, "./docker/statestore-committer"),
                pushImageCommand(expectedCommitterTag),
                buildImageCommand(expectedIngestTag, "./docker/ingest"),
                pushImageCommand(expectedIngestTag),
                buildImageCommand(expectedBulkImportTag, "./docker/bulk-import-runner"),
                pushImageCommand(expectedBulkImportTag),
                buildAndPushMultiplatformImageCommand(expectedCompactionTag, "./docker/compaction"),
                buildImageCommand(expectedEmrTag, "./docker/bulk-import-runner-emr-serverless"),
                pushImageCommand(expectedEmrTag));
    }

    @Test
    void shouldDisableCreatingBuildxBuilder() throws Exception {

        // Given
        DockerImageConfiguration dockerImageConfiguration = dockerDeploymentImageConfig();

        // When
        uploadAllImagesNoBuildxBuilder(dockerImageConfiguration);

        // Then
        String expectedCommitterTag = "www.somedocker.com/prefix/statestore-committer:1.0.0";
        String expectedIngestTag = "www.somedocker.com/prefix/ingest:1.0.0";
        String expectedBulkImportTag = "www.somedocker.com/prefix/bulk-import-runner:1.0.0";
        String expectedCompactionTag = "www.somedocker.com/prefix/compaction:1.0.0";
        String expectedEmrTag = "www.somedocker.com/prefix/bulk-import-runner-emr-serverless:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand(expectedCommitterTag, "./docker/statestore-committer"),
                pushImageCommand(expectedCommitterTag),
                buildImageCommand(expectedIngestTag, "./docker/ingest"),
                pushImageCommand(expectedIngestTag),
                buildImageCommand(expectedBulkImportTag, "./docker/bulk-import-runner"),
                pushImageCommand(expectedBulkImportTag),
                buildAndPushMultiplatformImageCommand(expectedCompactionTag, "./docker/compaction"),
                buildImageCommand(expectedEmrTag, "./docker/bulk-import-runner-emr-serverless"),
                pushImageCommand(expectedEmrTag));
    }

    @Test
    void shouldBuildAndPushLambdaImages() throws Exception {

        // Given
        files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");
        files.put(Path.of("./jars/ingest.jar"), "ingest-jar-content");
        files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");
        files.put(Path.of("./jars/athena.jar"), "athena-jar-content");

        DockerImageConfiguration dockerImageConfiguration = lambdaImageConfig();

        // When
        uploadAllImages(dockerImageConfiguration);

        // Then
        String expectedStatestoreTag = "www.somedocker.com/prefix/statestore-lambda:1.0.0";
        String expectedIngestTaskTag = "www.somedocker.com/prefix/ingest-task-creator-lambda:1.0.0";
        String expectedBulkImportTag = "www.somedocker.com/prefix/bulk-import-starter-lambda:1.0.0";
        String expectedAthenaTag = "www.somedocker.com/prefix/athena-lambda:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                buildLambdaImageCommand(expectedStatestoreTag, "./docker/lambda"),
                pushImageCommand(expectedStatestoreTag),
                buildLambdaImageCommand(expectedIngestTaskTag, "./docker/lambda"),
                pushImageCommand(expectedIngestTaskTag),
                buildLambdaImageCommand(expectedBulkImportTag, "./docker/lambda"),
                pushImageCommand(expectedBulkImportTag),
                buildLambdaImageCommand(expectedAthenaTag, "./docker/lambda"),
                pushImageCommand(expectedAthenaTag));

        assertThat(files).isEqualTo(Map.of(
                Path.of("./jars/statestore.jar"), "statestore-jar-content",
                Path.of("./jars/ingest.jar"), "ingest-jar-content",
                Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content",
                Path.of("./jars/athena.jar"), "athena-jar-content",
                Path.of("./docker/lambda/lambda.jar"), "athena-jar-content"));

    }

    @Test
    void shouldFailWhenDockerBuildFails() {
        // Given
        DockerImageConfiguration dockerImageConfiguration = dockerDeploymentImageConfig();
        CommandPipeline buildImageCommand = buildImageCommand(
                "www.somedocker.com/prefix/statestore-committer:1.0.0",
                "./docker/statestore-committer");
        setReturnExitCodeForCommand(42, buildImageCommand);

        // When / Then
        assertThatThrownBy(() -> uploadAllImages(dockerImageConfiguration))
                .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                    assertThat(e.getCommand()).isEqualTo(buildImageCommand);
                    assertThat(e.getExitCode()).isEqualTo(42);
                });
        assertThat(commandsThatRan).containsExactly(
                removeOldBuildxBuilderInstanceCommand(),
                createNewBuildxBuilderInstanceCommand(),
                buildImageCommand);
    }

    protected void uploadAllImages(DockerImageConfiguration imageConfig) throws Exception {
        uploadAllImages(imageConfig, uploaderBuilder().build());
    }

    protected void uploadAllImagesNoBuildxBuilder(DockerImageConfiguration imageConfig) throws Exception {
        uploadAllImages(imageConfig, uploaderBuilder().createMultiplatformBuilder(false).build());
    }

    protected void uploadAllImages(DockerImageConfiguration imageConfig, UploadDockerImages uploader) throws Exception {
        UploadDockerImagesToRepository.uploadAllImages(imageConfig, uploader, "www.somedocker.com/prefix");
    }

    protected UploadDockerImages.Builder uploaderBuilder() {
        return UploadDockerImages.builder()
                .commandRunner(commandRunner)
                .deployConfig(DeployConfiguration.fromLocalBuild())
                .copyFile((source, target) -> files.put(target, files.get(source)))
                .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                .version("1.0.0");
    }
}
