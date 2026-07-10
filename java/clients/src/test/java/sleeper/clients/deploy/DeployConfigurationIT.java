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
package sleeper.clients.deploy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class DeployConfigurationIT {

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectory(tempDir.resolve("templates"));
    }

    @Test
    void shouldReadConfigFile() throws Exception {
        // Given
        DeployConfiguration config = DeployConfiguration.fromLocalBuild()
                .withOverrideBaseImageDir(tempDir.resolve("base-image").toString());
        Files.writeString(DeployConfiguration.configFileInScriptsDirectory(tempDir),
                new DeployConfigurationSerDe().toJson(config));

        // When / Then
        assertThat(DeployConfiguration.fromScriptsDirectory(tempDir)).isEqualTo(config);
    }

    @Test
    void shouldDefaultToLocalBuildWhenConfigFileIsNotPresent() throws Exception {
        assertThat(DeployConfiguration.fromScriptsDirectory(tempDir))
                .isEqualTo(DeployConfiguration.fromLocalBuild());
    }
}
