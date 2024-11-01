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

package sleeper.core.deploy;

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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DeployInstanceConfigurationFromTemplatesIT {
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
            DeployInstanceConfiguration instanceConfiguration = fromTemplatesDirWithTable("set-table");

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "template-instance");
            expectedInstanceProperties.setTags(Map.of("Project", "TemplateProject"));
            TableProperties expectedTableProperties = new TableProperties(expectedInstanceProperties);
            expectedTableProperties.set(TABLE_NAME, "set-table");
            expectedTableProperties.setSchema(schemaWithKey("template-key"));
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
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
            DeployInstanceConfiguration instanceConfiguration = fromTemplatesDirWithTableAndSplits("test-table", splitPointsFile);

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
        @Test
        void shouldLoadTablePropertiesAndSchemaIfFoundNearInstanceProperties() throws Exception {
            // Given
            Files.writeString(propertiesDir.resolve("instance.properties"), "sleeper.id=test-instance");
            Files.writeString(propertiesDir.resolve("tags.properties"), "Project=TestProject");
            Files.writeString(propertiesDir.resolve("table.properties"), "sleeper.table.name=test-table");
            Files.writeString(propertiesDir.resolve("schema.json"), new SchemaSerDe().toJson(schemaWithKey("key")));

            // When
            DeployInstanceConfiguration instanceConfiguration = fromInstancePropertiesOrTemplatesDir(
                    propertiesDir.resolve("instance.properties"));

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
        void shouldLoadNoTablesIfNotFoundNearInstanceProperties() throws Exception {
            // Given
            Files.writeString(propertiesDir.resolve("instance.properties"), "sleeper.id=test-instance");

            // When
            DeployInstanceConfiguration instanceConfiguration = fromInstancePropertiesOrTemplatesDir(
                    propertiesDir.resolve("instance.properties"));

            // Then
            InstanceProperties expectedInstanceProperties = new InstanceProperties();
            expectedInstanceProperties.set(ID, "test-instance");
            assertThat(instanceConfiguration)
                    .isEqualTo(DeployInstanceConfiguration.builder()
                            .instanceProperties(expectedInstanceProperties)
                            .tableProperties(List.of())
                            .build());
        }
    }

    private void writeTemplates() throws IOException {
        Files.createDirectories(templatesDir);
        Files.writeString(templatesDir.resolve("instanceproperties.template"), "sleeper.id=template-instance");
        Files.writeString(templatesDir.resolve("tags.template"), "Project=TemplateProject");
        Files.writeString(templatesDir.resolve("tableproperties.template"), "sleeper.table.name=template-table");
        Files.writeString(templatesDir.resolve("schema.template"), new SchemaSerDe().toJson(schemaWithKey("template-key")));
    }

    private DeployInstanceConfiguration fromInstancePropertiesOrTemplatesDir(Path instancePropertiesFile) {
        return DeployInstanceConfigurationFromTemplates.builder()
                .instancePropertiesPath(instancePropertiesFile)
                .templatesDir(templatesDir)
                .build().load();
    }

    private DeployInstanceConfiguration fromTemplatesDirWithTable(String tableName) {
        return DeployInstanceConfigurationFromTemplates.builder()
                .templatesDir(templatesDir)
                .tableNameForTemplate(tableName)
                .build().load();
    }

    private DeployInstanceConfiguration fromTemplatesDirWithTableAndSplits(String tableName, Path splitPointsFile) {
        return DeployInstanceConfigurationFromTemplates.builder()
                .templatesDir(templatesDir)
                .tableNameForTemplate(tableName)
                .splitPointsFileForTemplate(splitPointsFile)
                .build().load();
    }
}
