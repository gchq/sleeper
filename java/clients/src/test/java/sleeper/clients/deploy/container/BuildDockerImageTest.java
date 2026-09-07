/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.util.cli.CommandArgumentReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.CopyOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildMultiplatformImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.createBuildxBuilderInstanceCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.useBuildxBuilderInstanceCommand;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class BuildDockerImageTest extends DockerImagesTestBase {
    protected final Map<String, String> files = new HashMap<>();

    @Test
    void shouldBuildDockerDeploymentImage() {
        // When
        buildImage(dockerDeploymentImageConfig(), "ingest", "test");

        // Then
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand("test", "./scripts/docker/ingest"));
    }

    @Test
    void shouldBuildMultiplatformImage() {
        // When
        buildImage(dockerDeploymentImageConfig(), "compaction", "test", "--multiplatform");

        // Then
        assertThat(commandsThatRan).containsExactly(
                createBuildxBuilderInstanceCommand(),
                useBuildxBuilderInstanceCommand(),
                buildMultiplatformImageCommand("test", "./scripts/docker/compaction"));
    }

    @Test
    void shouldBuildMultiplatformImageWithSinglePlatform() {
        // When
        buildImage(dockerDeploymentImageConfig(), "compaction", "test");

        // Then
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand("test", "./scripts/docker/compaction"));
    }

    @Test
    void shouldBuildLambdaImage() {
        // Given
        writeFile("./scripts/jars/statestore.jar", "jar-content");

        // When
        buildImage(lambdaImageConfig(), "statestore-lambda", "test");

        // Then
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand("test", "./scripts/docker/lambda"));
        assertThat(files).isEqualTo(Map.of(
                "./scripts/jars/statestore.jar", "jar-content",
                "./scripts/docker/lambda/lambda.jar", "jar-content"));
    }

    @Test
    void shouldBuildBaseImage() {
        // When
        buildImage(dockerDeploymentImageConfig(), "base", "test");

        // Then
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand("test", "./scripts/docker/base"));
    }

    @Test
    void shouldBuildBaseImageForMultiplePlatforms() {
        // When
        buildImage(dockerDeploymentImageConfig(), "base", "test", "--multiplatform");

        // Then
        assertThat(commandsThatRan).containsExactly(
                createBuildxBuilderInstanceCommand(),
                useBuildxBuilderInstanceCommand(),
                buildMultiplatformImageCommand("test", "./scripts/docker/base"));
    }

    @Test
    void shouldSetDefaultBaseImage() {
        // When
        buildImage(dockerDeploymentImageConfig(), "ingest", "test", "--default-base-image", "base:test");

        // Then
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand("test", "./scripts/docker/ingest", "base:test"));
    }

    @Test
    void shouldNotApplyDefaultBaseImageWhenNotUsed() {
        // When
        buildImage(dockerDeploymentImageConfig(), "bulk-import-runner", "test", "--default-base-image", "base:test");

        // Then
        assertThat(commandsThatRan).containsExactly(
                buildImageCommand("test", "./scripts/docker/bulk-import-runner"));
    }

    @Test
    void shouldPassThroughOptionToDocker() {
        // When
        buildImage(dockerDeploymentImageConfig(), "ingest", "test", "--no-cache");

        // Then
        assertThat(commandsThatRan).containsExactly(
                pipeline(command("docker", "build", "-t", "test", "--no-cache", "./scripts/docker/ingest")));
    }

    private void buildImage(DockerImageConfiguration config, String... args) {
        String[] allArgs = Stream.concat(Stream.of("./scripts"), Stream.of(args)).toArray(String[]::new);
        var arguments = BuildDockerImage.readArguments(CommandArgumentReader.parse(BuildDockerImage.USAGE, allArgs));
        try {
            BuildDockerImage.build(config, commandRunner, this::copyFile, arguments);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void writeFile(String path, String content) {
        files.put(path, content);
    }

    private void copyFile(Path source, Path target, CopyOption... options) throws IOException {
        String sourceContent = files.get(source.toString());
        if (sourceContent == null) {
            throw new FileNotFoundException("File not found: " + source);
        }
        if (!List.of(options).equals(List.of(StandardCopyOption.REPLACE_EXISTING))) {
            throw new IOException("Unexpected copy options: " + Arrays.toString(options));
        }
        files.put(target.toString(), sourceContent);
    }
}
