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

package sleeper.core.deploy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SleeperInstanceConfigurationIT {
    @TempDir
    private Path templatesDir;
    @TempDir
    private Path propertiesDir;

    @Nested
    @DisplayName("Load from template directory")
    class LoadFromTemplateDirectory {

        @Test
        void shouldLoadTemplatesWithTableName() throws Exception {
            // Given
            writeTemplates();

            // When
            SleeperInstanceConfiguration instanceConfiguration = fromTemplatesDirWithTable("set-table");

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(FILE_SYSTEM, "test://");
            expectedInstanceProperties.setTags(Map.of("Project", "TemplateProject"));
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "set-table");
            expectedTableProperties.setNumber(ROW_GROUP_SIZE, 123);
            expectedTableProperties.setSchema(createSchemaWithKey("template-key"));
            assertThat(instanceConfiguration)
                    .isEqualTo(SleeperInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(List.of(expectedTableProperties))
                            .build());
        }

        @Test
        void shouldSetSplitPointsFileInTemplate() throws Exception {
            // Given
            writeTemplates();
            Path splitPointsFile = Files.writeString(propertiesDir.resolve("splits.txt"), "abc\ndef");

            // When
            SleeperInstanceConfiguration instanceConfiguration = fromTemplatesDirWithTableAndSplits("test-table", splitPointsFile);

            // Then
            assertThat(instanceConfiguration.getTableProperties())
                    .extracting(properties -> properties.get(SPLIT_POINTS_FILE))
                    .containsExactly(splitPointsFile.toString());
        }

        @Test
        void shouldFailIfSplitPointsFileDoesNotExist() throws Exception {
            // Given
            writeTemplates();
            Path splitPointsFile = propertiesDir.resolve("splits.txt");

            // When / Then
            assertThatThrownBy(() -> fromTemplatesDirWithTableAndSplits("test-table", splitPointsFile))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("Split points file not found: ");
        }
    }

    @Nested
    @DisplayName("Load from instance properties")
    class LoadFromInstanceProperties {

        @BeforeEach
        void writeInstanceProperties() throws IOException {
            Files.writeString(propertiesDir.resolve("instance.properties"), "sleeper.id=test-instance");
        }

        @Test
        void shouldLoadTagsButNotTablesWhenGivenInstancePropertiesFile() throws Exception {
            // Given
            writeTagsProperties();
            writeTableFiles(propertiesDir, "test-table", "key");

            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfiguration(
                    propertiesDir.resolve("instance.properties"));

            // Then
            InstanceProperties expectedInstanceProperties = expectedInstanceProperties();
            expectedInstanceProperties.setTags(Map.of("Project", "TestProject"));
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties, List.of()));
        }

        @Test
        void shouldLoadNoTablesIfNotFoundNearInstanceProperties() throws Exception {
            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfiguration(
                    propertiesDir.resolve("instance.properties"));

            // Then
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties(), List.of()));
        }

        @Test
        void shouldLoadFromConfigurationDirectoryWithSingleTable() throws Exception {
            // Given
            writeTagsProperties();
            writeTableFiles(propertiesDir, "test-table", "key");

            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(propertiesDir);

            // Then
            InstanceProperties expectedInstanceProperties = expectedInstanceProperties();
            expectedInstanceProperties.setTags(Map.of("Project", "TestProject"));
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties,
                            expectedTableProperties(expectedInstanceProperties, "test-table", "key")));
        }

        @Test
        void shouldLoadFromConfigurationDirectoryWithTablesSubdirectory() throws Exception {
            // Given
            writeTableFiles(Files.createDirectories(propertiesDir.resolve("tables/table-1")), "table-1", "key1");
            writeTableFiles(Files.createDirectories(propertiesDir.resolve("tables/table-2")), "table-2", "key2");

            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(propertiesDir);

            // Then
            InstanceProperties expectedInstanceProperties = expectedInstanceProperties();
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties, List.of(
                            expectedTableProperties(expectedInstanceProperties, "table-1", "key1"),
                            expectedTableProperties(expectedInstanceProperties, "table-2", "key2"))));
        }

        @Test
        void shouldLoadFromConfigurationDirectoryWhenGivenInstancePropertiesFileWithinIt() throws Exception {
            // Given
            writeTableFiles(propertiesDir, "test-table", "key");

            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(
                    propertiesDir.resolve("instance.properties"));

            // Then
            InstanceProperties expectedInstanceProperties = expectedInstanceProperties();
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties,
                            expectedTableProperties(expectedInstanceProperties, "test-table", "key")));
        }

        @Test
        void shouldLoadFromConfigurationDirectoryWhenGivenNonStandardInstancePropertiesFilename() throws Exception {
            // Given
            Files.writeString(propertiesDir.resolve("custom-instance.properties"), "sleeper.id=test-instance");
            writeTableFiles(propertiesDir, "test-table", "key");

            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(
                    propertiesDir.resolve("custom-instance.properties"));

            // Then
            InstanceProperties expectedInstanceProperties = expectedInstanceProperties();
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties,
                            expectedTableProperties(expectedInstanceProperties, "test-table", "key")));
        }

        @Test
        void shouldLoadFromConfigurationDirectoryWithNoTables() throws Exception {
            // When
            SleeperInstanceConfiguration instanceConfiguration = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(propertiesDir);

            // Then
            assertThat(instanceConfiguration)
                    .isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties(), List.of()));
        }
    }

    @Nested
    @DisplayName("Create for a new instance, defaulting instance properties to the template")
    class CreateForNewInstanceDefaultingInstance {

        @Test
        void shouldPopulateInstancePropertiesFromLocalConfig() throws Exception {
            // Given
            writeTemplates();
            Path instancePropertiesPath = Files.writeString(
                    propertiesDir.resolve("instance.properties"),
                    "sleeper.filesystem=test://");

            // When
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingInstance(
                    instancePropertiesPath, templatesDir);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(FILE_SYSTEM, "test://");
            assertThat(config).isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties, List.of()));
        }

        @Test
        void shouldPopulateInstancePropertiesFromTemplate() throws Exception {
            // Given
            writeTemplates();
            // When
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingInstance(
                    null, templatesDir);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();

            Map<String, String> tags = new HashMap<>(expectedInstanceProperties.getTags());
            tags.put("Project", "TemplateProject");
            expectedInstanceProperties.setTags(tags);
            expectedInstanceProperties.set(FILE_SYSTEM, "test://");
            assertThat(config).isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties, List.of()));
        }
    }

    @Nested
    @DisplayName("Create for a new instance, defaulting table properties to the template")
    class CreateForNewInstanceDefaultingTables {

        @Test
        void shouldPopulateTablePropertiesFromLocalConfig() throws Exception {
            // Given
            writeTemplates();
            Path instancePropertiesPath = Files.writeString(
                    propertiesDir.resolve("instance.properties"),
                    "sleeper.filesystem=test://");
            Files.writeString(
                    propertiesDir.resolve("table.properties"),
                    "sleeper.table.name=some-table");
            SleeperInstanceConfigurationFromTemplates fromTemplates = SleeperInstanceConfigurationFromTemplates.builder()
                    .templatesDir(templatesDir)
                    .tableNameForTemplate("template-table").build();

            // When
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingTables(
                    instancePropertiesPath, fromTemplates);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(FILE_SYSTEM, "test://");
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "some-table");
            assertThat(config).isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties, expectedTableProperties));
        }

        @Test
        void shouldPopulateTablePropertiesFromTemplate() throws Exception {
            // Given
            writeTemplates();
            Path instancePropertiesPath = Files.writeString(
                    propertiesDir.resolve("instance.properties"),
                    "sleeper.filesystem=test://");
            SleeperInstanceConfigurationFromTemplates fromTemplates = SleeperInstanceConfigurationFromTemplates.builder()
                    .templatesDir(templatesDir)
                    .tableNameForTemplate("template-table").build();

            // When
            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingTables(
                    instancePropertiesPath, fromTemplates);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(FILE_SYSTEM, "test://");
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "template-table");
            expectedTableProperties.setNumber(ROW_GROUP_SIZE, 123);
            expectedTableProperties.setSchema(createSchemaWithKey("template-key"));
            assertThat(config).isEqualTo(new SleeperInstanceConfiguration(expectedInstanceProperties, expectedTableProperties));
        }
    }

    private void writeTagsProperties() throws IOException {
        Files.writeString(propertiesDir.resolve("tags.properties"), "Project=TestProject");
    }

    private void writeTableFiles(Path directory, String tableName, String schemaKey) throws IOException {
        Files.writeString(directory.resolve("table.properties"), "sleeper.table.name=" + tableName);
        Files.writeString(directory.resolve("schema.json"), new SchemaSerDe().toJson(createSchemaWithKey(schemaKey)));
    }

    private InstanceProperties expectedInstanceProperties() {
        InstanceProperties expected = new InstanceProperties();
        expected.set(ID, "test-instance");
        return expected;
    }

    private TableProperties expectedTableProperties(InstanceProperties instanceProperties, String tableName, String schemaKey) {
        TableProperties expected = new TableProperties(instanceProperties);
        expected.set(TABLE_NAME, tableName);
        expected.setSchema(createSchemaWithKey(schemaKey));
        return expected;
    }

    private void writeTemplates() throws IOException {
        Files.createDirectories(templatesDir);
        Files.writeString(templatesDir.resolve("instanceproperties.template"), "sleeper.filesystem=test://");
        Files.writeString(templatesDir.resolve("tags.template"), "Project=TemplateProject");
        Files.writeString(templatesDir.resolve("tableproperties.template"), "sleeper.table.parquet.rowgroup.size=123");
        Files.writeString(templatesDir.resolve("schema.template"), new SchemaSerDe().toJson(createSchemaWithKey("template-key")));
    }

    private SleeperInstanceConfiguration fromTemplatesDirWithTable(String tableName) {
        return SleeperInstanceConfigurationFromTemplates.builder()
                .templatesDir(templatesDir)
                .tableNameForTemplate(tableName)
                .build().load();
    }

    private SleeperInstanceConfiguration fromTemplatesDirWithTableAndSplits(String tableName, Path splitPointsFile) {
        return SleeperInstanceConfigurationFromTemplates.builder()
                .templatesDir(templatesDir)
                .tableNameForTemplate(tableName)
                .splitPointsFileForTemplate(splitPointsFile)
                .build().load();
    }
}
