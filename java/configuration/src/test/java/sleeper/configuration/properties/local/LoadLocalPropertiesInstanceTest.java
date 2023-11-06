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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.propertiesString;

class LoadLocalPropertiesInstanceTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    private Path tempDir;
    private Path instancePropertiesFile;

    @BeforeEach
    void setUp() {
        instancePropertiesFile = tempDir.resolve("instance.properties");
    }

    @Test
    void shouldLoadTagsFromTagsFileNextToInstancePropertiesFile() throws IOException {
        // Given
        instanceProperties.save(instancePropertiesFile);
        writeTagsFile(Map.of("tag-1", "value-1"));

        // When
        InstanceProperties loaded = loadInstanceProperties(instancePropertiesFile);

        // Then
        assertThat(loaded.getTags())
                .isEqualTo(Map.of("tag-1", "value-1"));
    }

    @Test
    void shouldSetNoTagsWhenTagsFileAndPropertyMissing() {
        // Given
        instanceProperties.save(instancePropertiesFile);

        // When
        InstanceProperties loaded = loadInstanceProperties(instancePropertiesFile);

        // Then
        assertThat(loaded.getTags())
                .isEmpty();
    }

    @Test
    void shouldKeepTagsPropertyWhenTagsFileMissing() {
        // Given
        instanceProperties.setTags(Map.of("tag-1", "property-value-1"));
        instanceProperties.save(instancePropertiesFile);

        // When
        InstanceProperties loaded = loadInstanceProperties(instancePropertiesFile);

        // Then
        assertThat(loaded.getTags())
                .isEqualTo(Map.of("tag-1", "property-value-1"));
    }

    @Test
    void shouldOverrideTagsWithValuesFromFile() throws IOException {
        // Given
        instanceProperties.setTags(Map.of(
                "tag-1", "property-value-1",
                "tag-2", "property-value-2"));
        instanceProperties.save(instancePropertiesFile);
        writeTagsFile(Map.of("tag-1", "file-value"));

        // When
        InstanceProperties loaded = loadInstanceProperties(instancePropertiesFile);

        // Then
        assertThat(loaded.getTags())
                .isEqualTo(Map.of("tag-1", "file-value"));
    }

    private void writeTagsFile(Map<String, String> tagMap) throws IOException {
        Properties tags = new Properties();
        tagMap.forEach(tags::setProperty);
        Files.writeString(tempDir.resolve("tags.properties"), propertiesString(tags));
    }

    private InstanceProperties loadInstanceProperties(Path file) {
        return LoadLocalProperties.loadInstanceProperties(InstanceProperties::new, file);
    }
}
