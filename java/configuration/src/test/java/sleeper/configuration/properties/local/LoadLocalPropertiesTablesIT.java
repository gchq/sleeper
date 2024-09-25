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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFile;
import static sleeper.core.properties.local.LoadLocalProperties.loadTablesFromInstancePropertiesFileNoValidation;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class LoadLocalPropertiesTablesIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    private Path tempDir;
    private Path instancePropertiesFile;

    @BeforeEach
    void setUp() {
        instancePropertiesFile = tempDir.resolve("instance.properties");
        instanceProperties.save(instancePropertiesFile);
    }

    private Stream<TableProperties> loadTableProperties() {
        return loadTablePropertiesWithInstancePropertiesFile(instancePropertiesFile);
    }

    private Stream<TableProperties> loadTablePropertiesNoValidation() {
        return loadTablesFromInstancePropertiesFileNoValidation(instanceProperties, instancePropertiesFile);
    }

    private Stream<TableProperties> loadTablePropertiesWithInstancePropertiesFile(Path file) {
        return loadTablesFromInstancePropertiesFile(instanceProperties, file);
    }

    @Test
    void shouldLoadFileNextToInstancePropertiesFile() throws IOException {
        // Given
        TableProperties properties = createTestTablePropertiesWithNoSchema(instanceProperties);
        properties.save(tempDir.resolve("table.properties"));
        Schema schema = schemaWithKey("test-key");
        schema.save(tempDir.resolve("schema.json"));

        // When / Then
        properties.setSchema(schema);
        assertThat(loadTableProperties())
                .containsExactly(properties);
    }

    @Test
    void shouldLoadFilesInTablesFolder() throws IOException {
        // Given
        Files.createDirectories(tempDir.resolve("tables/table1"));
        Files.createDirectory(tempDir.resolve("tables/table2"));

        TableProperties properties1 = createTestTablePropertiesWithNoSchema(instanceProperties);
        properties1.save(tempDir.resolve("tables/table1/table.properties"));
        TableProperties properties2 = createTestTablePropertiesWithNoSchema(instanceProperties);
        properties2.save(tempDir.resolve("tables/table2/table.properties"));

        Schema schema1 = schemaWithKey("test-key1");
        schema1.save(tempDir.resolve("tables/table1/schema.json"));
        Schema schema2 = schemaWithKey("test-key2");
        schema2.save(tempDir.resolve("tables/table2/schema.json"));

        // When / Then
        properties1.setSchema(schema1);
        properties2.setSchema(schema2);
        assertThat(loadTableProperties())
                .containsExactly(properties1, properties2);
    }

    @Test
    void shouldFindNoneWhenNoFilesArePresent() {
        // When / Then
        assertThat(loadTableProperties())
                .isEmpty();
    }

    @Test
    void shouldFindNoneWhenInstancePropertiesPathDoesNotIncludeParentDirectoryAndNothingInWorkingDirectory() {
        // When / Then
        assertThat(loadTablePropertiesWithInstancePropertiesFile(
                Paths.get("instance.properties")))
                .isEmpty();
    }

    @Test
    void shouldFailToLoadWhenNoSchemaSpecified() {
        // Given
        TableProperties properties = createTestTablePropertiesWithNoSchema(instanceProperties);
        properties.save(tempDir.resolve("table.properties"));

        // When/Then
        assertThatThrownBy(() -> loadTableProperties()
                .forEach(table -> {
                    // Consume the stream to trigger reading the properties file
                }))
                .hasMessage("Property sleeper.table.schema was invalid. It was unset.");
    }

    @Test
    void shouldLoadInvalidProperties() {
        // Given
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        properties.unset(TABLE_NAME);
        properties.save(tempDir.resolve("table.properties"));

        // When
        TableProperties tableProperties = loadTablePropertiesNoValidation().findFirst().orElseThrow();

        // Then
        assertThat(tableProperties.get(TABLE_NAME)).isNull();
    }

    @Test
    void shouldLoadInvalidPropertiesWithNoSchema() {
        // Given
        TableProperties properties = createTestTablePropertiesWithNoSchema(instanceProperties);
        properties.save(tempDir.resolve("table.properties"));

        // When
        TableProperties tableProperties = loadTablePropertiesNoValidation().findFirst().orElseThrow();

        // Then
        assertThat(tableProperties.getSchema()).isNull();
    }

    @Test
    void shouldLoadFileNextToInstancePropertiesFileWithSchemaInProperties() {
        // Given
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("test-key"));
        properties.save(tempDir.resolve("table.properties"));

        // When / Then
        assertThat(loadTableProperties())
                .containsExactly(properties);
    }

    @Test
    void shouldLoadFilesInTablesFolderWithSchemaInProperties() throws IOException {
        // Given
        Files.createDirectories(tempDir.resolve("tables/table1"));
        Files.createDirectory(tempDir.resolve("tables/table2"));
        TableProperties properties1 = createTestTableProperties(instanceProperties, schemaWithKey("test-key1"));
        properties1.save(tempDir.resolve("tables/table1/table.properties"));
        TableProperties properties2 = createTestTableProperties(instanceProperties, schemaWithKey("test-key2"));
        properties2.save(tempDir.resolve("tables/table2/table.properties"));

        // When / Then
        assertThat(loadTableProperties())
                .containsExactly(properties1, properties2);
    }

    @Test
    void shouldLoadWhitespaceFromSchemaJson() throws IOException {
        // Given
        TableProperties properties = createTestTablePropertiesWithNoSchema(instanceProperties);
        properties.save(tempDir.resolve("table.properties"));
        String schemaWithNewlines = "{\"rowKeyFields\":[{\n" +
                "\"name\":\"key\",\"type\":\"LongType\"\n" +
                "}],\n" +
                "\"sortKeyFields\":[],\n" +
                "\"valueFields\":[]}";
        Files.writeString(tempDir.resolve("schema.json"), schemaWithNewlines);

        // When / Then
        assertThat(loadTableProperties())
                .extracting(table -> table.get(SCHEMA))
                .containsExactly(schemaWithNewlines);
    }
}
