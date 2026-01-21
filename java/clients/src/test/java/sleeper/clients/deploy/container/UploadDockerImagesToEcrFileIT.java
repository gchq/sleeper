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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.core.properties.model.LambdaDeployType;
import sleeper.core.properties.model.OptionalStack;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildLambdaImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.dockerLoginToEcrCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.pushImageCommand;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class UploadDockerImagesToEcrFileIT extends UploadDockerImagesToEcrTestBase {

    @TempDir
    public Path dir;
    private Path dockerDir;
    private Path jarsDir;
    private Path lambdaImageDir;

    @BeforeEach
    void setUp() throws Exception {
        dockerDir = Files.createDirectories(dir.resolve("docker"));
        jarsDir = Files.createDirectories(dir.resolve("jars"));
        lambdaImageDir = Files.createDirectories(dockerDir.resolve("lambda"));
    }

    @Test
    void shouldUploadTwoLambdaImagesOverwritingJarEachTime() throws Exception {
        // Given
        properties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.IngestStack));
        properties.set(LAMBDA_DEPLOY_TYPE, LambdaDeployType.CONTAINER.toString());
        Files.writeString(jarsDir.resolve("statestore.jar"), "statestore-jar-content");
        Files.writeString(jarsDir.resolve("ingest.jar"), "ingest-jar-content");

        // When
        uploadForDeployment(lambdaImageConfig());

        // Then
        String expectedTag1 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/statestore-lambda:1.0.0";
        String expectedTag2 = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest-task-creator-lambda:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                dockerLoginToEcrCommand(),
                buildLambdaImageCommand(expectedTag1, lambdaImageDir.toString()),
                pushImageCommand(expectedTag1),
                buildLambdaImageCommand(expectedTag2, lambdaImageDir.toString()),
                pushImageCommand(expectedTag2));

        assertThat(fileToContentUnder(dir)).isEqualTo(Map.of(
                jarsDir.resolve("statestore.jar"), "statestore-jar-content",
                jarsDir.resolve("ingest.jar"), "ingest-jar-content",
                lambdaImageDir.resolve("lambda.jar"), "ingest-jar-content"));
    }

    @Override
    protected UploadDockerImagesToEcr uploader() {
        return new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .commandRunner(commandRunner)
                        .deployConfig(DeployConfiguration.fromLocalBuild())
                        .baseDockerDirectory(dockerDir).jarsDirectory(jarsDir)
                        .version("1.0.0")
                        .build(),
                ecrClient, "123", "test-region");
    }

    private static Map<Path, String> fileToContentUnder(Path directory) throws Exception {
        return filesUnder(directory).stream()
                .collect(toUnmodifiableMap(file -> file, file -> readString(file)));
    }

    private static List<Path> filesUnder(Path directory) {
        try (Stream<Path> list = Files.list(directory)) {
            return list.flatMap(path -> {
                if (Files.isDirectory(path)) {
                    return filesUnder(path).stream();
                } else {
                    return Stream.of(path);
                }
            }).collect(toUnmodifiableList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String readString(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
