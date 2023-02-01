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
package sleeper.cdk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;

class UtilsTablePropertiesTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    private Path tempDir;
    private Path instancePropertiesFile;

    @BeforeEach
    void setUp() throws IOException {
        instancePropertiesFile = tempDir.resolve("instance.properties");
        instanceProperties.save(instancePropertiesFile);
    }

    private Schema schemaWithKey(String keyName) {
        return Schema.builder().rowKeyFields(new Field(keyName, new StringType())).build();
    }

    @Test
    void shouldFindTableConfigurationNextToInstancePropertiesFile() throws IOException {
        // Given
        Schema schema = schemaWithKey("test-key");
        TableProperties properties = createTestTablePropertiesWithNoSchema(instanceProperties);
        schema.save(tempDir.resolve("schema.json"));
        properties.save(tempDir.resolve("table.properties"));

        // When / Then
        properties.setSchema(schema);
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .containsExactly(properties);
    }

    @Test
    void shouldFindTablePropertiesFilesInTablesFolder() throws IOException {
        // Given
        Files.createDirectories(tempDir.resolve("tables/table1"));
        Files.createDirectory(tempDir.resolve("tables/table2"));
        TableProperties properties1 = createTestTableProperties(instanceProperties, schemaWithKey("test-key1"));
        properties1.save(tempDir.resolve("tables/table1/table.properties"));
        TableProperties properties2 = createTestTableProperties(instanceProperties, schemaWithKey("test-key2"));
        properties2.save(tempDir.resolve("tables/table2/table.properties"));

        // When / Then
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .containsExactly(properties1, properties2);
    }

    @Test
    void shouldFindNoTablePropertiesFilesWhenNonePresent() throws IOException {
        // When / Then
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .isEmpty();
    }

    @Test
    void shouldFindNothingInWorkingDirectoryWhenInstancePropertiesDirectoryNotSpecified() throws IOException {
        // When / Then
        assertThat(Utils.getAllTableProperties(instanceProperties, Paths.get("instance.properties")))
                .isEmpty();
    }

    @Test
    void shouldFindTablePropertiesFileNextToInstancePropertiesFileWithSchemaInProperties() throws IOException {
        // Given
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("test-key"));
        properties.save(tempDir.resolve("table.properties"));

        // When / Then
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .containsExactly(properties);
    }

    @Test
    void shouldFindTablePropertiesFilesInTablesFolderWithSchemaInProperties() throws IOException {
        // Given
        Files.createDirectories(tempDir.resolve("tables/table1"));
        Files.createDirectory(tempDir.resolve("tables/table2"));
        TableProperties properties1 = createTestTableProperties(instanceProperties, schemaWithKey("test-key1"));
        properties1.save(tempDir.resolve("tables/table1/table.properties"));
        TableProperties properties2 = createTestTableProperties(instanceProperties, schemaWithKey("test-key2"));
        properties2.save(tempDir.resolve("tables/table2/table.properties"));

        // When / Then
        assertThat(Utils.getAllTableProperties(instanceProperties, instancePropertiesFile))
                .containsExactly(properties1, properties2);
    }
}
