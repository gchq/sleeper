/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.configuration.properties.local;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

class SaveLocalPropertiesTest {
    @TempDir
    public Path folder;

    @Test
    void shouldLoadInstancePropertiesFromLocalDirectory() throws IOException {
        // Given
        Path path = Path.of(createTempDirectory(folder, null).toString() + "/instance.properties");
        InstanceProperties properties = createTestInstanceProperties();
        properties.save(path);

        // When
        SaveLocalProperties configuration = SaveLocalProperties.loadFromPath(path);

        // Then
        assertThat(configuration.getInstanceProperties())
                .isEqualTo(properties);
    }

    @Test
    void shouldFailToLoadInstancePropertiesIfFileNotFound() throws IOException {
        // Given
        Path path = Path.of(createTempDirectory(folder, null).toString() + "/instance.properties");

        // When/Then
        assertThatThrownBy(() -> SaveLocalProperties.loadFromPath(path))
                .isInstanceOf(UncheckedIOException.class);
    }
}
