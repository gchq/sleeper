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

import sleeper.clients.deploy.DeployConfiguration;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class UploadDockerImagesResolveBuildContextTest extends DockerImagesTestBase {

    @Test
    void shouldResolveBaseImageBuildContextFromDefaultDirectoryWhenNoOverrideSet() {
        // Given
        UploadDockerImages uploader = uploaderBuilder(DeployConfiguration.fromLocalBuild()).build();

        // When
        Path context = uploader.resolveBuildContext(baseImage());

        // Then
        assertThat(context).isEqualTo(Path.of("./docker/base"));
    }

    @Test
    void shouldResolveBaseImageBuildContextFromOverrideDirectoryWhenSet() {
        // Given
        UploadDockerImages uploader = uploaderBuilder(
                DeployConfiguration.fromLocalBuildWithOverrideBaseImageDir("./custom/base"))
                .build();

        // When
        Path context = uploader.resolveBuildContext(baseImage());

        // Then
        assertThat(context).isEqualTo(Path.of("./custom/base"));
    }

    @Test
    void shouldResolveNonBaseImageBuildContextFromDefaultDirectoryEvenWhenOverrideSet() {
        // Given
        UploadDockerImages uploader = uploaderBuilder(
                DeployConfiguration.fromLocalBuildWithOverrideBaseImageDir("./custom/base"))
                .build();
        StackDockerImage ingest = StackDockerImage.dockerBuildImage("ingest");

        // When
        Path context = uploader.resolveBuildContext(ingest);

        // Then
        assertThat(context).isEqualTo(Path.of("./docker/ingest"));
    }

    private UploadDockerImages.Builder uploaderBuilder(DeployConfiguration deployConfig) {
        return UploadDockerImages.builder()
                .commandRunner(commandRunner)
                .deployConfig(deployConfig)
                .baseDockerDirectory(Path.of("./docker"))
                .jarsDirectory(Path.of("./jars"))
                .baseImage(baseImage())
                .version("1.0.0");
    }
}
