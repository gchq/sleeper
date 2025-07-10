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

import org.junit.jupiter.api.Test;

import sleeper.clients.util.command.CommandPipeline;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;

public class UploadDockerImagesToRepositoryTest extends DockerImagesTestBase {

    protected final Map<Path, String> files = new HashMap<>();

    @Test
    void shouldBuildAndPushECSImages() throws Exception {

        // Given
        DockerImageConfiguration dockerImageConfiguration = ecsImageConfig();

        // When
        List<CommandPipeline> commandsThatRan = pipelinesRunOn(
                runCommand -> uploader().upload(runCommand, dockerImageConfiguration));

        // Then
        String expectedTag = "www.somedocker.com/ingest:1.0.0";
        String expectedTag2 = "www.somedocker.com/bulk-import-runner:1.0.0";
        String expectedTag3 = "www.somedocker.com/compaction:1.0.0";
        String expectedTag4 = "www.somedocker.com/bulk-import-runner-emr-serverless:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                removeOldBuildxBuilderInstanceCommand(),
                createNewBuildxBuilderInstanceCommand(),
                buildImageCommand(expectedTag, "./docker/ingest"),
                pushImageCommand(expectedTag),
                buildImageCommand(expectedTag2, "./docker/bulk-import-runner"),
                pushImageCommand(expectedTag2),
                buildAndPushImageWithBuildxCommand(expectedTag3, "./docker/compaction"),
                buildImageCommand(expectedTag4, "./docker/bulk-import-runner-emr-serverless"),
                pushImageCommand(expectedTag4));
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
