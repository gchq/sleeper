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
package sleeper.clients.report.table;

import org.junit.jupiter.api.Test;

import sleeper.clients.api.InMemorySleeperInstance;
import sleeper.clients.api.SleeperClient;
import sleeper.clients.report.ListTablesReport;
import sleeper.clients.report.tables.JsonListTablesReporter;
import sleeper.clients.report.tables.StandardListTablesReporter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class ListTablesTest {

    @Test
    public void shouldReportZeroStandardTables() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
        SleeperClient sleeperClient = instance.sleeperClientBuilder().build();

        // When
        String output = runStandardListTableReport(sleeperClient);

        // Then
        assertThat(output).isEqualTo("");
    }

    @Test
    public void shouldReportZeroJSONTables() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
        SleeperClient sleeperClient = instance.sleeperClientBuilder().build();

        // When
        String output = runJSONListTableReport(sleeperClient);

        // Then
        String expected = "{\n" +
                "  \"tables\": []\n" +
                "}\n";
        assertThat(output).isEqualTo(expected);
    }

    @Test
    public void shouldReportOneStandardTable() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Schema schema = createSchemaWithKey("key", new StringType());
        InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
        SleeperClient sleeperClient = instance.sleeperClientBuilder().build();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "test_table_1");
        tableProperties.set(TableProperty.TABLE_ID, "0123abc");
        sleeperClient.addTable(tableProperties, List.of());

        // When
        String output = runStandardListTableReport(sleeperClient);

        // Then
        assertThat(output).isEqualTo("\"test_table_1\"\t\"0123abc\"\n");
    }

    @Test
    public void shouldReportOneJSONTable() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Schema schema = createSchemaWithKey("key", new StringType());
        InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
        SleeperClient sleeperClient = instance.sleeperClientBuilder().build();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "test_table_1");
        tableProperties.set(TableProperty.TABLE_ID, "0123abc");
        sleeperClient.addTable(tableProperties, List.of());

        // When
        String output = runJSONListTableReport(sleeperClient);

        // Then
        String expected = "{\n" +
                "  \"tables\": [\n" +
                "    {\n" +
                "      \"name\": \"test_table_1\",\n" +
                "      \"id\": \"0123abc\"\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";
        assertThat(output).isEqualTo(expected);
    }

    @Test
    public void shouldReportMultipleStandardTables() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Schema schema = createSchemaWithKey("key", new StringType());
        InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
        SleeperClient sleeperClient = instance.sleeperClientBuilder().build();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "test_table_1");
        tableProperties.set(TableProperty.TABLE_ID, "0123abc");
        sleeperClient.addTable(tableProperties, List.of());

        tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "test_table_2");
        tableProperties.set(TableProperty.TABLE_ID, "9876def");
        sleeperClient.addTable(tableProperties, List.of());

        tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "some_other_table");
        tableProperties.set(TableProperty.TABLE_ID, "3456bcde");
        sleeperClient.addTable(tableProperties, List.of());

        // When
        String output = runStandardListTableReport(sleeperClient);

        // Then
        assertThat(output).isEqualTo("\"some_other_table\"\t\"3456bcde\"\n\"test_table_1\"\t\"0123abc\"\n\"test_table_2\"\t\"9876def\"\n");
    }

    @Test
    public void shouldReportMultipleJSONTables() {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Schema schema = createSchemaWithKey("key", new StringType());
        InMemorySleeperInstance instance = new InMemorySleeperInstance(instanceProperties);
        SleeperClient sleeperClient = instance.sleeperClientBuilder().build();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "test_table_1");
        tableProperties.set(TableProperty.TABLE_ID, "0123abc");
        sleeperClient.addTable(tableProperties, List.of());

        tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "test_table_2");
        tableProperties.set(TableProperty.TABLE_ID, "9876def");
        sleeperClient.addTable(tableProperties, List.of());

        tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, "some_other_table");
        tableProperties.set(TableProperty.TABLE_ID, "3456bcde");
        sleeperClient.addTable(tableProperties, List.of());

        // When
        String output = runJSONListTableReport(sleeperClient);

        // Then
        String expected = "{\n" +
                "  \"tables\": [\n" +
                "    {\n" +
                "      \"name\": \"some_other_table\",\n" +
                "      \"id\": \"3456bcde\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"test_table_1\",\n" +
                "      \"id\": \"0123abc\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"test_table_2\",\n" +
                "      \"id\": \"9876def\"\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";
        assertThat(output).isEqualTo(expected);
    }

    private static String runStandardListTableReport(SleeperClient client) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ListTablesReport report = new ListTablesReport(client, new StandardListTablesReporter(new PrintStream(stream)));
        report.run();
        return stream.toString(StandardCharsets.UTF_8);
    }

    private static String runJSONListTableReport(SleeperClient client) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ListTablesReport report = new ListTablesReport(client, new JsonListTablesReporter(new PrintStream(stream)));
        report.run();
        return stream.toString(StandardCharsets.UTF_8);
    }
}
