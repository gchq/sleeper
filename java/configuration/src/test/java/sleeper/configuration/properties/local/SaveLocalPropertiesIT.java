/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.local.LoadLocalProperties.loadInstanceProperties;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.core.properties.local.SaveLocalProperties.saveToDirectory;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class SaveLocalPropertiesIT {
    @TempDir
    private Path tempDir;

    @Test
    void shouldSaveInstanceProperties() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        // When
        saveToDirectory(tempDir, properties, Stream.empty());

        // Then
        assertThat(loadInstanceProperties(tempDir.resolve("instance.properties")))
                .isEqualTo(properties);
    }

    @Test
    void shouldSaveTableProperties() throws IOException {
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
    void shouldLoadNoTablePropertiesWhenNoneSaved() throws IOException {
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
    void shouldSaveSchemaFile() throws IOException {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        Schema schema = Schema.builder().rowKeyFields(new Field("test-key", new LongType())).build();
        TableProperties tableProperties = createTestTableProperties(properties, schema);
        tableProperties.set(TABLE_NAME, "test-table");

        // When
        saveToDirectory(tempDir, properties, Stream.of(tableProperties));

        // Then
        assertThat(Schema.load(tempDir.resolve("tables/test-table/schema.json")))
                .isEqualTo(schema);
    }
}
