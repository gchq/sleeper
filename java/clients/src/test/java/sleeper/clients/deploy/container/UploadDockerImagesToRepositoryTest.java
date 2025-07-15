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

import sleeper.clients.util.command.CommandFailedException;
import sleeper.clients.util.command.CommandPipeline;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Upload Docker images")
public class UploadDockerImagesToRepositoryTest extends DockerImagesTestBase {

    protected final Map<Path, String> files = new HashMap<>();

    @Test
    void shouldBuildAndPushContainerImages() throws Exception {

        // Given
        DockerImageConfiguration dockerImageConfiguration = dockerDeploymentImageConfig();

        // When
        uploadAllImages(dockerImageConfiguration);

        // Then
        String expectedIngestTag = "www.somedocker.com/ingest:1.0.0";
        String expectedBulkImportTag = "www.somedocker.com/bulk-import-runner:1.0.0";
        String expectedCompactionTag = "www.somedocker.com/compaction:1.0.0";
        String expectedEmrTag = "www.somedocker.com/bulk-import-runner-emr-serverless:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand(expectedIngestTag, "./docker/ingest"),
                pushImageCommand(expectedIngestTag),
                buildImageCommand(expectedBulkImportTag, "./docker/bulk-import-runner"),
                pushImageCommand(expectedBulkImportTag),
                buildAndPushMultiplatformImageCommand(expectedCompactionTag, "./docker/compaction"),
                buildImageCommand(expectedEmrTag, "./docker/bulk-import-runner-emr-serverless"),
                pushImageCommand(expectedEmrTag));
    }

    @Test
    void shouldBuildAndPushLambdas() throws Exception {

        // Given
        files.put(Path.of("./jars/statestore.jar"), "statestore-jar-content");
        files.put(Path.of("./jars/ingest.jar"), "ingest-jar-content");
        files.put(Path.of("./jars/bulk-import-starter.jar"), "bulk-import-starter-jar-content");
        files.put(Path.of("./jars/athena.jar"), "athena-jar-content");

        DockerImageConfiguration dockerImageConfiguration = lambdaImageConfig();

        // When
        uploadAllImages(dockerImageConfiguration);

        // Then
        String expectedStatestoreTag = "www.somedocker.com/statestore-lambda:1.0.0";
        String expectedIngestTaskTag = "www.somedocker.com/ingest-task-creator-lambda:1.0.0";
        String expectedBulkImportTag = "www.somedocker.com/bulk-import-starter-lambda:1.0.0";
        String expectedAthenaTag = "www.somedocker.com/athena-lambda:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                buildImageCommandWithArgs("-t", expectedStatestoreTag, "./docker/lambda"),
                pushImageCommand(expectedStatestoreTag),
                buildImageCommandWithArgs("-t", expectedIngestTaskTag, "./docker/lambda"),
                pushImageCommand(expectedIngestTaskTag),
                buildImageCommandWithArgs("-t", expectedBulkImportTag, "./docker/lambda"),
                pushImageCommand(expectedBulkImportTag),
                buildImageCommandWithArgs("-t", expectedAthenaTag, "./docker/lambda"),
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
                "www.somedocker.com/ingest:1.0.0",
                "./docker/ingest");
        setReturnExitCodeForCommand(42, buildImageCommand);

        // When / Then
        assertThatThrownBy(() -> uploadAllImages(dockerImageConfiguration))
                .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                    assertThat(e.getCommand()).isEqualTo(buildImageCommand);
                    assertThat(e.getExitCode()).isEqualTo(42);
                });
        assertThat(commandsThatRan).containsExactly(buildImageCommand);
    }

    protected void uploadAllImages(DockerImageConfiguration imageConfig) throws Exception {
        uploader().upload(UploadDockerImagesRequest.allImagesToRepository("www.somedocker.com", imageConfig));
    }

    protected UploadDockerImagesToRepository uploader() {
        return UploadDockerImagesToRepository.builder()
                .commandRunner(commandRunner)
                .copyFile((source, target) -> files.put(target, files.get(source)))
                .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                .version("1.0.0")
                .build();
    }
}
