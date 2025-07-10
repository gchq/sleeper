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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;
import static sleeper.clients.testutil.RunCommandTestHelper.returningExitCodeForCommand;

@DisplayName("Upload Docker images")
public class UploadDockerImagesToRepositoryTest extends DockerImagesTestBase {

    protected final Map<Path, String> files = new HashMap<>();

    @Test
    void shouldBuildAndPushContainerImages() throws Exception {

        // Given
        DockerImageConfiguration dockerImageConfiguration = containerImageConfig();

        // When
        List<CommandPipeline> commandsThatRan = pipelinesRunOn(
                runCommand -> uploader().upload(runCommand, dockerImageConfiguration));

        // Then
        String expectedIngestTag = "www.somedocker.com/ingest:1.0.0";
        String expectedBulkImportTag = "www.somedocker.com/bulk-import-runner:1.0.0";
        String expectedCompactionTag = "www.somedocker.com/compaction:1.0.0";
        String expectedEmrTag = "www.somedocker.com/bulk-import-runner-emr-serverless:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                removeOldBuildxBuilderInstanceCommand(),
                createNewBuildxBuilderInstanceCommand(),
                buildImageCommand(expectedIngestTag, "./docker/ingest"),
                pushImageCommand(expectedIngestTag),
                buildImageCommand(expectedBulkImportTag, "./docker/bulk-import-runner"),
                pushImageCommand(expectedBulkImportTag),
                buildAndPushImageWithBuildxCommand(expectedCompactionTag, "./docker/compaction"),
                buildImageCommand(expectedEmrTag, "./docker/bulk-import-runner-emr-serverless"),
                pushImageCommand(expectedEmrTag));
    }

    @Test
    void shouldBuildAndPushLambdas() throws Exception {

        // Given
        DockerImageConfiguration dockerImageConfiguration = lambdaImageConfig();

        // When
        List<CommandPipeline> commandsThatRan = pipelinesRunOn(
                runCommand -> uploader().upload(runCommand, dockerImageConfiguration));

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
    }

    @Test
    void shouldFailWhenCreateBuildxBuilderFails() {
        // Given
        DockerImageConfiguration dockerImageConfiguration = containerImageConfig();

        // When / Then
        assertThatThrownBy(() -> uploader().upload(
                returningExitCodeForCommand(123, createNewBuildxBuilderInstanceCommand()),
                dockerImageConfiguration))
                .isInstanceOfSatisfying(CommandFailedException.class, e -> {
                    assertThat(e.getCommand()).isEqualTo(createNewBuildxBuilderInstanceCommand());
                    assertThat(e.getExitCode()).isEqualTo(123);
                });
    }

    protected UploadDockerImagesToRepository uploader() {
        return UploadDockerImagesToRepository.builder()
                .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                .repositoryHost("www.somedocker.com")
                .copyFile((source, target) -> files.put(target, files.get(source)))
                .version("1.0.0")
                .build();
    }
}
