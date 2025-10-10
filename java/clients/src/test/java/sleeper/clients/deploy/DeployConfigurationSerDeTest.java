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
package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DeployConfigurationSerDeTest {

    DeployConfigurationSerDe serDe = new DeployConfigurationSerDe();

    @Test
    void shouldSerDeDeployFromLocalBuild() {
        // Given
        DeployConfiguration configuration = DeployConfiguration.fromLocalBuild();

        // When
        String json = serDe.toJson(configuration);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(configuration);
        assertThat(json).isEqualTo("""
                {
                  "dockerImageLocation": "localBuild"
                }""");
    }

    @Test
    void shouldSerDeDeployFromDockerRepository() {
        // Given
        DeployConfiguration configuration = DeployConfiguration.fromDockerRepository("ghcr.io/gchq/");

        // When
        String json = serDe.toJson(configuration);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(configuration);
        assertThat(json).isEqualTo("""
                {
                  "dockerImageLocation": "repository",
                  "dockerRepositoryPrefix": "ghcr.io/gchq/"
                }""");
    }

    @Test
    void shouldNotDeserialiseWhenImageLocationIsNotRecognised() {
        // Given
        String json = """
                {
                  "dockerImageLocation": "abc"
                }
                """;

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(json))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldDeserialiseImageLocationCaseInsensitive() {
        // Given
        String json = """
                {
                  "dockerImageLocation": "loCalbuIld"
                }
                """;

        // When / Then
        assertThat(serDe.fromJson(json))
                .isEqualTo(DeployConfiguration.fromLocalBuild());
    }

    @Test
    void shouldFailWhenImageLocationIsMissing() {
        // Given
        String json = "{}";

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(json))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldFailWhenRepositoryIsMissing() {
        // Given
        String json = """
                {
                  "dockerImageLocation": "repository"
                }
                """;

        // When / Then
        assertThatThrownBy(() -> serDe.fromJson(json))
                .isInstanceOf(RuntimeException.class);
    }
}
