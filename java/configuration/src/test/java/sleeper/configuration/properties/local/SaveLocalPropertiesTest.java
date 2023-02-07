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
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SleeperProperties.loadProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadInstanceProperties;
import static sleeper.configuration.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.configuration.properties.local.SaveLocalProperties.saveToDirectory;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class SaveLocalPropertiesTest {
    @TempDir
    private Path tempDir;

    @Test
    void shouldSaveInstanceProperties() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        // When
        saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(loadInstanceProperties(new InstanceProperties(), tempDir.resolve("instance.properties")))
                .isEqualTo(properties);
    }

    @Test
    void shouldSaveTableProperties() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(properties, schemaWithKey("key"));

        // When
        saveToDirectory(tempDir, properties, Stream.of(tableProperties));

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(properties, tempDir.resolve("instance.properties")))
                .containsExactly(tableProperties);
    }

    @Test
    void shouldLoadNoTablePropertiesWhenNoneSaved() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        // When
        saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(loadTablesFromInstancePropertiesFile(properties, tempDir.resolve("instance.properties")))
                .isEmpty();
    }

    @Test
    void shouldSaveTagsFile() throws IOException {
        // Given
        Properties tags = new Properties();
        tags.setProperty("tag-1", "value-1");
        tags.setProperty("tag-2", "value-2");
        InstanceProperties properties = createTestInstanceProperties();
        properties.loadTags(tags);

        // When
        saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(loadProperties(tempDir.resolve("tags.properties")))
                .isEqualTo(tags);
    }

    @Test
    void shouldSaveConfigBucketFile() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(CONFIG_BUCKET, "test-config-bucket");

        // When
        saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(Files.readString(tempDir.resolve("configBucket.txt")))
                .isEqualTo("test-config-bucket");
    }

    @Test
    void shouldSaveQueryBucketFile() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(QUERY_RESULTS_BUCKET, "test-query-bucket");

        // When
        saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(Files.readString(tempDir.resolve("queryBucket.txt")))
                .isEqualTo("test-query-bucket");
    }
}
