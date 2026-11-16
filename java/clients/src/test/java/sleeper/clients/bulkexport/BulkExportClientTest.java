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
package sleeper.clients.bulkexport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_S3_BUCKET;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class BulkExportClientTest {
    private static final String PROMPT_SQL = "Enter SQL statement (blank for none): ";
    private static final String PROMPT_ANOTHER = "Submit another bulk export? (y/n) ";
    private static final String PROMPT_TABLE = "Which table do you wish to export? ";

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final List<BulkExportQuery> sent = new ArrayList<>();
    private final Iterator<String> exportIds = List.of("export-1", "export-2").iterator();

    @BeforeEach
    void setUp() {
        instanceProperties.set(BULK_EXPORT_QUEUE_URL, "export-queue-url");
        instanceProperties.set(BULK_EXPORT_S3_BUCKET, "export-bucket");
    }

    @Test
    void shouldExportTableWithNoSql() {
        // Given
        TableProperties table = createTable("test-table", DataEngine.DATAFUSION);
        in.enterNextPrompts("", "n");

        // When
        runClient();

        // Then
        assertThat(sent).containsExactly(BulkExportQuery.builder()
                .tableName("test-table").exportId("export-1").build());
        assertThat(out.toString())
                .startsWith("Exporting table test-table\n")
                .contains(PROMPT_SQL)
                .contains("Submitted bulk export with id export-1\n" +
                        "Results will be written to s3://export-bucket/" + table.get(TABLE_ID) + "/export-1/\n")
                .endsWith(PROMPT_ANOTHER + "\n");
    }

    @Test
    void shouldIncludeSqlStatement() {
        // Given
        createTable("test-table", DataEngine.DATAFUSION);
        in.enterNextPrompts("SELECT key FROM query_results", "n");

        // When
        runClient();

        // Then
        assertThat(sent).containsExactly(BulkExportQuery.builder()
                .tableName("test-table").exportId("export-1")
                .sqlQuery("SELECT key FROM query_results").build());
    }

    @Test
    void shouldNotPromptForSqlWhenTableUsesJavaDataEngine() {
        // Given
        createTable("test-table", DataEngine.JAVA);
        in.enterNextPrompts("n");

        // When
        runClient();

        // Then
        assertThat(sent).containsExactly(BulkExportQuery.builder()
                .tableName("test-table").exportId("export-1").build());
        assertThat(out.toString())
                .contains("The table uses the Java data engine, so an SQL statement cannot be applied to the export.")
                .doesNotContain(PROMPT_SQL);
    }

    @Test
    void shouldPromptForTableWhenThereAreSeveral() {
        // Given
        createTable("table-1", DataEngine.DATAFUSION);
        createTable("table-2", DataEngine.DATAFUSION);
        in.enterNextPrompts("not-a-table", "table-2", "", "n");

        // When
        runClient();

        // Then
        assertThat(sent).containsExactly(BulkExportQuery.builder()
                .tableName("table-2").exportId("export-1").build());
        assertThat(out.toString())
                .startsWith("The system contains the following tables:\n" +
                        "table-1\n" +
                        "table-2\n" +
                        PROMPT_TABLE + "\n" +
                        "Invalid table, try again\n");
    }

    @Test
    void shouldSubmitAnotherExportWithNewId() {
        // Given
        createTable("test-table", DataEngine.DATAFUSION);
        in.enterNextPrompts("", "y", "SELECT key FROM query_results", "n");

        // When
        runClient();

        // Then
        assertThat(sent).containsExactly(
                BulkExportQuery.builder().tableName("test-table").exportId("export-1").build(),
                BulkExportQuery.builder().tableName("test-table").exportId("export-2")
                        .sqlQuery("SELECT key FROM query_results").build());
    }

    @Test
    void shouldDoNothingWhenThereAreNoTables() {
        // When
        runClient();

        // Then
        assertThat(sent).isEmpty();
        assertThat(out.toString())
                .isEqualTo("There are no tables. Please create one and add data before running this class.\n");
    }

    @Test
    void shouldFailWhenBulkExportIsNotDeployed() {
        // Given
        instanceProperties.unset(BULK_EXPORT_QUEUE_URL);
        createTable("test-table", DataEngine.DATAFUSION);

        // When / Then
        assertThatThrownBy(this::runClient)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires the bulk export stack to have been deployed");
        assertThat(sent).isEmpty();
    }

    private void runClient() {
        new BulkExportClient(instanceProperties, tableIndex, new TablePropertiesProvider(instanceProperties, tablePropertiesStore),
                sent::add, in.consoleIn(), out.consoleOut(), exportIds::next)
                .run();
    }

    private TableProperties createTable(String tableName, DataEngine dataEngine) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setEnum(DATA_ENGINE, dataEngine);
        tablePropertiesStore.save(tableProperties);
        return tableProperties;
    }
}
