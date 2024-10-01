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

package sleeper.core.properties.deploy;

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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DeployInstanceConfigurationIT {
    @TempDir
    private Path tempDir;

    @Nested
    @DisplayName("Load from template directory")
    class LoadFromTemplateDirectory {
        @Test
        void shouldLoadInstanceAndTableProperties() throws Exception {
            // Given
            createTemplatesInDirectory(tempDir);

            // When
            DeployInstanceConfiguration instanceConfiguration = DeployInstanceConfigurationFromTemplates.builder()
                    .templatesDir(tempDir).build().load();

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "template-instance");
            expectedInstanceProperties.setTags(Map.of("Project", "TemplateProject"));
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "template-table");
            expectedTableProperties.setSchema(schemaWithKey("template-key"));
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(expectedTableProperties)
                            .build());
        }

        @Test
        void shouldSetTableNameInTemplate() throws Exception {
            // Given
            createTemplatesInDirectory(tempDir);

            // When
            DeployInstanceConfiguration instanceConfiguration = DeployInstanceConfigurationFromTemplates.builder()
                    .templatesDir(tempDir).tableNameForTemplate("set-table").build().load();

            // Then
            assertThat(instanceConfiguration.getTableProperties())
                    .extracting(properties -> properties.get(TABLE_NAME))
                    .containsExactly("set-table");
        }

        @Test
        void shouldSetSplitPointsFileInTemplate() throws Exception {
            // Given
            createTemplatesInDirectory(tempDir);
            Path splitPointsFile = Files.writeString(tempDir.resolve("splits.txt"), "abc\ndef");

            // When
            DeployInstanceConfiguration instanceConfiguration = DeployInstanceConfigurationFromTemplates.builder()
                    .templatesDir(tempDir).splitPointsFileForTemplate(splitPointsFile).build().load();

            // Then
            assertThat(instanceConfiguration.getTableProperties())
                    .extracting(properties -> properties.get(SPLIT_POINTS_FILE))
                    .containsExactly(splitPointsFile.toString());
        }

        @Test
        void shouldFailIfSplitPointsFileDoesNotExist() throws Exception {
            // Given
            createTemplatesInDirectory(tempDir);

            // When
            DeployInstanceConfigurationFromTemplates fromTemplates = DeployInstanceConfigurationFromTemplates.builder()
                    .templatesDir(tempDir).splitPointsFileForTemplate(tempDir.resolve("splits.txt")).build();

            // Then
            assertThatThrownBy(fromTemplates::load)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("Split points file not found: ");
        }
    }

    @Nested
    @DisplayName("Load from instance properties")
    class LoadFromInstanceProperties {
        @Test
        void shouldLoadTablePropertiesAndSchemaIfFoundNearInstanceProperties() throws Exception {
            // Given
            Path templateDir = createTemplatesInDirectory(tempDir.resolve("templates"));
            Files.writeString(tempDir.resolve("instance.properties"), "sleeper.id=test-instance");
            Files.writeString(tempDir.resolve("tags.properties"), "Project=TestProject");
            Files.writeString(tempDir.resolve("table.properties"), "sleeper.table.name=test-table");
            Files.writeString(tempDir.resolve("schema.json"), new SchemaSerDe().toJson(schemaWithKey("key")));

            // When
            DeployInstanceConfiguration instanceConfiguration = fromInstancePropertiesOrTemplatesDir(
                    tempDir.resolve("instance.properties"), templateDir);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "test-instance");
            expectedInstanceProperties.setTags(Map.of("Project", "TestProject"));
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "test-table");
            expectedTableProperties.setSchema(schemaWithKey("key"));
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(expectedTableProperties)
                            .build());
        }

        @Test
        void shouldLoadTableAndTagsFromTemplateDirectoryIfNotFoundNearInstanceProperties() throws Exception {
            // Given
            Path templateDir = createTemplatesInDirectory(tempDir.resolve("templates"));
            Files.writeString(tempDir.resolve("instance.properties"), "sleeper.id=test-instance");
            Path splitPointsFile = Files.writeString(tempDir.resolve("splits.txt"), "abc\ndef");

            // When
            DeployInstanceConfiguration instanceConfiguration = DeployInstanceConfigurationFromTemplates.builder()
                    .instancePropertiesPath(tempDir.resolve("instance.properties"))
                    .templatesDir(templateDir)
                    .tableNameForTemplate("test-table")
                    .splitPointsFileForTemplate(splitPointsFile)
                    .build().load();

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "test-instance");
            expectedInstanceProperties.setTags(Map.of("Project", "TemplateProject"));
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "test-table");
            expectedTableProperties.setSchema(schemaWithKey("template-key"));
            expectedTableProperties.set(SPLIT_POINTS_FILE, splitPointsFile.toString());
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(expectedTableProperties)
                            .build());
        }

        @Test
        void shouldLoadSchemaFromTemplateDirectoryIfNotSetInLocalProperties() throws Exception {
            // Given
            Path templateDir = createTemplatesInDirectory(tempDir.resolve("templates"));
            Files.writeString(tempDir.resolve("instance.properties"), "sleeper.id=test-instance");
            Files.writeString(tempDir.resolve("tags.properties"), "Project=TestProject");
            Files.writeString(tempDir.resolve("table.properties"), "sleeper.table.name=test-table");

            // When
            DeployInstanceConfiguration instanceConfiguration = fromInstancePropertiesOrTemplatesDir(
                    tempDir.resolve("instance.properties"), templateDir);

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "test-instance");
            expectedInstanceProperties.setTags(Map.of("Project", "TestProject"));
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "test-table");
            expectedTableProperties.setSchema(schemaWithKey("template-key"));
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(expectedTableProperties)
                            .build());
        }
    }

    private static Path createTemplatesInDirectory(Path templatesDir) throws IOException {
        Files.createDirectories(templatesDir);
        Files.writeString(templatesDir.resolve("instanceproperties.template"), "sleeper.id=template-instance");
        Files.writeString(templatesDir.resolve("tags.template"), "Project=TemplateProject");
        Files.writeString(templatesDir.resolve("tableproperties.template"), "sleeper.table.name=template-table");
        Files.writeString(templatesDir.resolve("schema.template"), new SchemaSerDe().toJson(schemaWithKey("template-key")));
        return templatesDir;
    }

    private DeployInstanceConfiguration fromInstancePropertiesOrTemplatesDir(Path instancePropertiesFile, Path templateDir) {
        return DeployInstanceConfigurationFromTemplates.builder()
                .instancePropertiesPath(instancePropertiesFile)
                .templatesDir(templateDir)
                .build().load();
    }
}
