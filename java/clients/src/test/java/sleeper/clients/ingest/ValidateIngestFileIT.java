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
package sleeper.clients.ingest;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.parquet.row.ParquetRowWriterFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

class ValidateIngestFileIT {
    @TempDir
    Path tempDir;

    private final Configuration configuration = new Configuration();
    private final TablePropertiesStore tables = InMemoryTableProperties.getStore();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final ValidateIngestFile client = new ValidateIngestFile(tables, configuration, out.consoleOut());
    private final Schema keySchema = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();

    @Test
    void shouldReportCompatibleFileFromCommandArguments() throws IOException {
        createTable(keySchema);
        Path file = writeFile(keySchema, new Row(Map.of("key", 1)));

        boolean compatible = run("instance", "test-table", file.toString());

        assertThat(compatible).isTrue();
        assertThat(out.toString()).contains(
                "File: " + file,
                "Table: test-table",
                "Schema is compatible with standard ingest.",
                "Row values and file data were not scanned");
    }

    @Test
    void shouldReportIncompatibleFileFromCommandArguments() throws IOException {
        createTable(keySchema);
        Schema fileSchema = Schema.builder().rowKeyFields(new Field("other", new IntType())).build();
        Path file = writeFile(fileSchema, new Row(Map.of("other", 1)));

        boolean compatible = run("instance", "test-table", file.toString());

        assertThat(compatible).isFalse();
        assertThat(out.toString()).contains(
                "Schema is not compatible with standard ingest:",
                "Missing non-nullable field 'key'.");
    }

    private boolean run(String... rawArgs) throws IOException {
        return client.run(CommandArgumentReader.parse(ValidateIngestFile.USAGE, rawArgs));
    }

    private void createTable(Schema schema) {
        TableProperties properties = createTestTableProperties(createTestInstanceProperties(), schema);
        properties.set(TABLE_NAME, "test-table");
        tables.createTable(properties);
    }

    private Path writeFile(Schema schema, Row row) throws IOException {
        Path file = tempDir.resolve("input.parquet");
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                new org.apache.hadoop.fs.Path(file.toUri()), schema)) {
            writer.write(row);
        }
        return file;
    }
}
