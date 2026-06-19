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

package sleeper.clients.table;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.table.AddTable.Arguments;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class AddTableArgumentsTest {

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldAcceptTableNameWithSchemaFile() {
            Arguments args = new Arguments("instance-id", "my-table", Path.of("schema.json"), null, null);
            assertThat(args.tableName()).isEqualTo("my-table");
        }

        @Test
        void shouldAcceptTablePropertiesFileAsTableNameSource() {
            Arguments args = new Arguments("instance-id", null, Path.of("schema.json"), Path.of("table.properties"), null);
            assertThat(args.tablePropertiesFile()).isEqualTo(Path.of("table.properties"));
        }

        @Test
        void shouldAcceptConfigDirAsTableNameAndSchemaSource() {
            Arguments args = new Arguments("instance-id", null, null, null, Path.of("config"));
            assertThat(args.configDir()).isEqualTo(Path.of("config"));
        }

        @Test
        void shouldRejectWhenNoTableNameSourceExists() {
            assertThatThrownBy(() -> new Arguments("instance-id", null, Path.of("schema.json"), null, null))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("A table name is required. Provide --table-name, or set it in --table-properties or --config-dir.");
        }

        @Test
        void shouldRejectWhenNoSchemaSource() {
            assertThatThrownBy(() -> new Arguments("instance-id", "my-table", null, null, null))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --schema or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenAllThreeFileSourcesSpecified() {
            assertThatThrownBy(() -> new Arguments("instance-id", "my-table",
                    Path.of("schema.json"), Path.of("table.properties"), Path.of("config")))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot specify --schema, --table-properties, and --config-dir together");
        }

        @Test
        void shouldResolveSchemaFileFromSchemaOption() {
            Arguments args = new Arguments("instance-id", "my-table", Path.of("schema.json"), null, null);
            assertThat(args.resolveSchemaFile()).isEqualTo(Path.of("schema.json"));
        }

        @Test
        void shouldResolveSchemaFileFromConfigDir() {
            Arguments args = new Arguments("instance-id", null, null, null, Path.of("config"));
            assertThat(args.resolveSchemaFile()).isEqualTo(Path.of("config", "schema.json"));
        }
    }

    @Nested
    class CreateTableProperties {

        private final InstanceProperties instanceProperties = createTestInstanceProperties();

        @TempDir
        private Path tempDir;

        @Test
        void shouldSetTableNameFromOption() throws IOException {
            Arguments args = new Arguments("instance-id", "test-table", tempDir.resolve("schema.json"), null, null);

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("test-table");
        }

        @Test
        void shouldLoadTableNameFromPropertiesFile() throws IOException {
            Path propsFile = tempDir.resolve("table.properties");
            Files.writeString(propsFile, "sleeper.table.name=file-table\n");
            Arguments args = new Arguments("instance-id", null, tempDir.resolve("schema.json"), propsFile, null);

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("file-table");
        }

        @Test
        void shouldOverrideTableNameInPropertiesFileWithOption() throws IOException {
            Path propsFile = tempDir.resolve("table.properties");
            Files.writeString(propsFile, "sleeper.table.name=file-table\n");
            Arguments args = new Arguments("instance-id", "override-table", tempDir.resolve("schema.json"), propsFile, null);

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("override-table");
        }

        @Test
        void shouldLoadTableNameFromConfigDir() throws IOException {
            Path configDir = tempDir.resolve("config");
            Files.createDirectories(configDir);
            Files.writeString(configDir.resolve("table.properties"), "sleeper.table.name=config-table\n");
            Arguments args = new Arguments("instance-id", null, null, null, configDir);

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("config-table");
        }

        @Test
        void shouldOverrideTableNameInConfigDirWithOption() throws IOException {
            Path configDir = tempDir.resolve("config");
            Files.createDirectories(configDir);
            Files.writeString(configDir.resolve("table.properties"), "sleeper.table.name=config-table\n");
            Arguments args = new Arguments("instance-id", "override-table", null, null, configDir);

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("override-table");
        }

        @Test
        void shouldReturnNoTableNameWhenNotSetInPropertiesFile() throws IOException {
            Path propsFile = tempDir.resolve("table.properties");
            Files.writeString(propsFile, "");
            Arguments args = new Arguments("instance-id", null, tempDir.resolve("schema.json"), propsFile, null);

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isNullOrEmpty();
        }
    }
}
