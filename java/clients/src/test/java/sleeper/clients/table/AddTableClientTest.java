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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class AddTableClientTest {
    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("my-instance");
    Schema schema = createSchemaWithKey("key");
    InMemoryTableIndex tableIndex = new InMemoryTableIndex();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());
    Map<String, InstanceProperties> instanceIdToProperties = new HashMap<>();
    Map<Path, String> pathToString = new HashMap<>();

    @BeforeEach
    void setUp() {
        instanceIdToProperties.put("my-instance", instanceProperties);
        saveSchemaFile("./schema.json", schema);
        saveFile("./table.properties", "sleeper.table.name=file-table\n");
    }

    @Nested
    class AddTable {

        @Test
        void shouldAddTableByNameAndSchema() throws Exception {
            // When
            addTable("my-instance", "--table-name", "my-table", "--schema", "./schema.json");

            // Then
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("my-table"));
            expected.set(TABLE_NAME, "my-table");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }

        @Test
        void shouldAddTableByPropertiesAndSchema() throws Exception {
            // When
            addTable("my-instance", "--table-properties", "./table.properties", "--schema", "./schema.json");

            // Then
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("file-table"));
            expected.set(TABLE_NAME, "file-table");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }

        @Test
        void shouldAddTableByConfigDir() throws Exception {
            //When
            addTable("my-instance", "--config-dir", "./");

            // Then
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("file-table"));
            expected.set(TABLE_NAME, "file-table");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }

        @Test
        void shouldOverrideTableNameFromOption() throws Exception {
            // When
            addTable("my-instance", "--table-name", "new-name", "--table-properties", "./table.properties", "--schema", "./schema.json");

            // Then
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("new-name"));
            expected.set(TABLE_NAME, "new-name");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }

        @Test
        void shouldOverrideTableNameInConfigDirWithOption() throws Exception {
            // When
            addTable("my-instance", "--table-name", "new-name", "--config-dir", "./");

            // Then
            TableProperties expected = new TableProperties(instanceProperties);
            expected.setSchema(schema);
            expected.set(TABLE_ID, tableId("new-name"));
            expected.set(TABLE_NAME, "new-name");
            assertThat(tablePropertiesStore.streamAllTables()).containsExactly(expected);
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectWhenNoTableNameSourceExists() {
            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--schema", "schema.json"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Table name was not found. Provide --table-name, or set it in --table-properties or --config-dir.");
        }

        @Test
        void shouldRejectWhenTableNameNotSetInPropertiesFile() throws IOException {
            //Given
            saveFile("other/table.properties", "sleeper.other.property=value\n");

            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--schema", "schema.json",
                    "--table-properties", "other/table.properties"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Table name was not found. Provide --table-name, or set it in --table-properties or --config-dir.");
        }

        @Test
        void shouldRejectWhenTableNameNotSetInConfigDir() throws IOException {
            //Given
            saveFile("other/table.properties", "sleeper.other.property=value\n");
            saveSchemaFile("other/schema.json", schema);

            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--config-dir", "other/"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Table name was not found. Provide --table-name, or set it in --table-properties or --config-dir.");
        }

        @Test
        void shouldRejectWhenNoSchemaSource() {
            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--table-name", "my-table"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Either --schema or --config-dir must be provided");
        }

        @Test
        void shouldRejectWhenNoTablePropertiesInConfigDir() {
            //Given
            saveSchemaFile("other/schema.json", schema);

            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--config-dir", "other/"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("No table.properties file was found at the supplied location. " +
                            "Provide a path to one using --table-properties or add one to the config directory specified by --config-dir.");
        }

        @Test
        void shouldRejectWhenNoSchemaInConfigDir() {
            //Given
            saveFile("other/table.properties", "sleeper.table.name=no-schema\n");

            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--config-dir", "other/"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("No schema.json file was found at the supplied location. " +
                            "Provide a path to one using --schema or add one to the config directory specified by --config-dir.");
        }

        @Test
        void shouldRejectWhenAllThreeFileSourcesSpecified() throws IOException {
            //When/Then
            assertThatThrownBy(() -> addTable("my-instance", "--table-name", "my-table",
                    "--schema", "schema.json", "--table-properties", "./table.properties",
                    "--config-dir", "./"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot specify --schema, --table-properties, and --config-dir together");
        }

    }

    private void addTable(String... args) throws Exception {
        var arguments = AddTableClient.readArguments(CommandArgumentReader.parse(AddTableClient.USAGE, args), this::readFile);
        TableProperties tableProperties = AddTableClient.createTablePropertiesWithLoaders(arguments, this::loadInstanceProperties, this::readFile);
        new AddTableClient(tableProperties, tablePropertiesStore, stateStoreProvider).run();
    }

    private void saveSchemaFile(String path, Schema schema) {
        pathToString.put(Path.of(path), new SchemaSerDe().toJson(schema));
    }

    private void saveFile(String path, String content) {
        pathToString.put(Path.of(path), content);
    }

    private String tableId(String tableName) {
        return tableIndex.getTableByName(tableName)
                .orElseThrow(() -> new RuntimeException("Found tables: " + tableIndex.streamAllTables().toList()))
                .getTableUniqueId();
    }

    private InstanceProperties loadInstanceProperties(String instanceId) {
        return Optional.ofNullable(instanceIdToProperties.get(instanceId))
                .orElseThrow();
    }

    private String readFile(Path path) throws IOException {
        try {
            return Optional.ofNullable(pathToString.get(path)).orElseThrow();
        } catch (NoSuchElementException e) {
            throw new IOException(e);
        }
    }
}
