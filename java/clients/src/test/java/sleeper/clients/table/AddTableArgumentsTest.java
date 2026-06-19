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
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class AddTableArgumentsTest {

    @Nested
    class ArgumentsValidation {

        @TempDir
        Path tempDir;

        @Test
        void shouldAcceptTableNameWithSchemaFile() {
            Arguments args = AddTable.parseArguments("instance-id", "--table-name", "my-table", "--schema", "schema.json");

            assertThat(args.tableName()).isEqualTo("my-table");
        }

        @Test
        void shouldAcceptTablePropertiesFileAsTableNameSource() throws IOException {
            Path tableProps = Files.writeString(tempDir.resolve("table.properties"), "sleeper.table.name=file-table\n");

            Arguments args = AddTable.parseArguments("instance-id", "--schema", "schema.json",
                    "--table-properties", tableProps.toString());

            assertThat(args.rawTableProperties().getProperty("sleeper.table.name")).isEqualTo("file-table");
        }

        @Test
        void shouldAcceptConfigDirAsTableNameAndSchemaSource() throws IOException {
            Files.writeString(tempDir.resolve("table.properties"), "sleeper.table.name=config-table\n");

            Arguments args = AddTable.parseArguments("instance-id", "--config-dir", tempDir.toString());

            assertThat(args.configDir()).isEqualTo(tempDir);
        }

        @Test
        void shouldRejectWhenNoTableNameSourceExists() {
            assertThatThrownBy(() -> AddTable.parseArguments("instance-id", "--schema", "schema.json"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Table name was not found. Provide --table-name, or set it in --table-properties or --config-dir.");
        }

        @Test
        void shouldRejectWhenTableNameNotSetInPropertiesFile() throws IOException {
            Path tableProps = Files.writeString(tempDir.resolve("table.properties"), "sleeper.other.property=value\n");

            assertThatThrownBy(() -> AddTable.parseArguments("instance-id", "--schema", "schema.json",
                    "--table-properties", tableProps.toString()))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Table name was not found. Provide --table-name, or set it in --table-properties or --config-dir.");
        }

        @Test
        void shouldRejectWhenNoSchemaSource() {
            assertThatThrownBy(() -> AddTable.parseArguments("instance-id", "--table-name", "my-table"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --schema or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenAllThreeFileSourcesSpecified() throws IOException {
            Path tableProps = Files.writeString(tempDir.resolve("table.properties"), "sleeper.table.name=my-table\n");

            assertThatThrownBy(() -> AddTable.parseArguments("instance-id", "--table-name", "my-table",
                    "--schema", "schema.json", "--table-properties", tableProps.toString(),
                    "--config-dir", tempDir.toString()))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot specify --schema, --table-properties, and --config-dir together");
        }

        @Test
        void shouldResolveSchemaFileFromSchemaOption() {
            Arguments args = AddTable.parseArguments("instance-id", "--table-name", "my-table", "--schema", "schema.json");
            assertThat(args.resolveSchemaFile()).isEqualTo(Path.of("schema.json"));
        }

        @Test
        void shouldResolveSchemaFileFromConfigDir() throws IOException {
            Files.writeString(tempDir.resolve("table.properties"), "sleeper.table.name=any-table\n");
            Arguments args = AddTable.parseArguments("instance-id", "--config-dir", tempDir.toString());
            assertThat(args.resolveSchemaFile()).isEqualTo(tempDir.resolve("schema.json"));
        }
    }

    @Nested
    class CreateTableProperties {
        private static Properties tablePropertiesWithName(String name) {
            Properties props = new Properties();
            props.setProperty("sleeper.table.name", name);
            return props;
        }

        private static Arguments withTableNameAndSchema(String tableName, String schemaFile) {
            return new Arguments("instance-id", tableName, Path.of(schemaFile), null, null, null);
        }

        private static Arguments withTablePropertiesAndSchema(Properties props, String schemaFile) {
            return new Arguments("instance-id", null, Path.of(schemaFile), props, Path.of("table.properties"), null);
        }

        private static Arguments withTableNameAndTablePropertiesAndSchema(String tableName, Properties props, String schemaFile) {
            return new Arguments("instance-id", tableName, Path.of(schemaFile), props, Path.of("table.properties"), null);
        }

        private static Arguments withConfigDir(Properties props, String configDir) {
            return new Arguments("instance-id", null, null, props, null, Path.of(configDir));
        }

        private static Arguments withTableNameAndConfigDir(String tableName, Properties props, String configDir) {
            return new Arguments("instance-id", tableName, null, props, null, Path.of(configDir));
        }

        private final InstanceProperties instanceProperties = createTestInstanceProperties();

        @Test
        void shouldSetTableNameFromOption() {
            Arguments args = withTableNameAndSchema("test-table", "schema.json");

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("test-table");
        }

        @Test
        void shouldLoadTableNameFromPropertiesFile() {
            Arguments args = withTablePropertiesAndSchema(tablePropertiesWithName("file-table"), "schema.json");

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("file-table");
        }

        @Test
        void shouldOverrideTableNameInPropertiesFileWithOption() {
            Arguments args = withTableNameAndTablePropertiesAndSchema("override-table", tablePropertiesWithName("file-table"), "schema.json");

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("override-table");
        }

        @Test
        void shouldLoadTableNameFromConfigDir() {
            Arguments args = withConfigDir(tablePropertiesWithName("config-table"), "config");

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("config-table");
        }

        @Test
        void shouldOverrideTableNameInConfigDirWithOption() {
            Arguments args = withTableNameAndConfigDir("override-table", tablePropertiesWithName("config-table"), "config");

            TableProperties result = AddTable.createTableProperties(instanceProperties, args);

            assertThat(result.get(TABLE_NAME)).isEqualTo("override-table");
        }
    }
}
