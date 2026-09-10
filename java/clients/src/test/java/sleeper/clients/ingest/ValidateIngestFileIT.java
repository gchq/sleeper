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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
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
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.parquet.row.ParquetRowReaderFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void shouldAcceptMatchingFileWithExtraColumns() throws IOException {
        createTable(keySchema);
        Path file = writeFile("required int64 extra; required int32 key;", group -> group.append("extra", 10L).append("key", 1));
        assertThat(check(file)).isTrue();
        assertThat(out.toString()).contains("Schema is compatible", "Row values and file data were not scanned", "Spark bulk import is not validated");
        assertThat(readFirst(file, keySchema)).isEqualTo(new Row(java.util.Map.of("key", 1)));
    }

    @Test
    void shouldRejectNullableKeyEvenWhenValuesAreNotNull() throws IOException {
        createTable(keySchema);
        Path file = writeFile("optional int32 key;", group -> group.append("key", 1));
        assertThat(check(file)).isFalse();
        assertThat(out.toString()).contains("'key' is nullable in the file but non-nullable in the table");
        assertThatThrownBy(() -> readFirst(file, keySchema)).isInstanceOf(ParquetDecodingException.class);
    }

    @Test
    void shouldValidateSchemaOfEmptyFile() throws IOException {
        createTable(keySchema);
        Path file = writeFile("optional int32 key;", null);
        assertThat(check(file)).isFalse();
        assertThat(out.toString()).contains("nullable in the file");
    }

    @Test
    void shouldReportAllMismatchedFields() throws IOException {
        createTable(Schema.builder().rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType())).build());
        Path file = writeFile("required int64 key; required binary value;", group -> group.append("key", 1L).append("value", "x"));
        assertThat(check(file)).isFalse();
        assertThat(out.toString()).contains("Field 'key'", "Field 'value'", "incompatible types");
    }

    @Test
    void shouldRejectMissingNonNullableField() throws IOException {
        createTable(keySchema);
        Path file = writeFile("required int32 other;", group -> group.append("other", 1));
        assertThat(check(file)).isFalse();
        assertThat(out.toString()).contains("Missing non-nullable field 'key'");
    }

    @Test
    void shouldAllowMissingNullableValue() throws IOException {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType(), true)).build();
        createTable(schema);
        Path file = writeFile("required int32 key;", group -> group.append("key", 1));
        assertThat(check(file)).isTrue();
        Row row = readFirst(file, schema);
        assertThat(row.get("key")).isEqualTo(1);
        assertThat(row.get("value")).isNull();
    }

    @Test
    void shouldAllowRequiredFileColumnForNullableValue() throws IOException {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType(), true)).build();
        createTable(schema);
        Path file = writeFile("required binary value (UTF8); required int32 key;", group -> group.append("value", "x").append("key", 1));
        assertThat(check(file)).isTrue();
        assertThat(readFirst(file, schema).get("value")).isEqualTo("x");
    }

    @Test
    void shouldRejectRepeatedScalar() throws IOException {
        createTable(keySchema);
        Path file = writeFile("repeated int32 key;", group -> group.append("key", 1).append("key", 2));
        assertThat(check(file)).isFalse();
        assertThatThrownBy(() -> readFirst(file, keySchema)).isInstanceOf(ParquetDecodingException.class);
    }

    @Test
    void shouldFailForNonParquetFile() throws IOException {
        createTable(keySchema);
        Path file = Files.writeString(tempDir.resolve("not-parquet.txt"), "This is not a Parquet file.");
        assertThatThrownBy(() -> check(file)).isInstanceOf(RuntimeException.class);
        assertThat(out.toString()).doesNotContain("Schema is compatible");
    }

    @Test
    void shouldFailForMissingFile() {
        createTable(keySchema);
        assertThatThrownBy(() -> check(tempDir.resolve("missing.parquet"))).isInstanceOf(IOException.class);
    }

    @Test
    void shouldFailForUnknownTableBeforeReadingFile() {
        assertThatThrownBy(() -> check(tempDir.resolve("missing.parquet"))).isInstanceOf(TableNotFoundException.class);
    }

    @Test
    void shouldRejectIncompleteArguments() {
        assertThatThrownBy(() -> client.run("instance", "table")).isInstanceOf(CommandArgumentsException.class);
    }

    private boolean check(Path file) throws IOException {
        return client.run("instance", "test-table", file.toString());
    }

    private void createTable(Schema schema) {
        TableProperties properties = createTestTableProperties(createTestInstanceProperties(), schema);
        properties.set(TABLE_NAME, "test-table");
        tables.createTable(properties);
    }

    private Path writeFile(String fields, Consumer<Group> populate) throws IOException {
        Path file = tempDir.resolve("input file.parquet");
        MessageType schema = MessageTypeParser.parseMessageType("message input {" + fields + "}");
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(file.toUri()))
                .withConf(configuration).withType(schema).build()) {
            if (populate != null) {
                Group group = new SimpleGroupFactory(schema).newGroup();
                populate.accept(group);
                writer.write(group);
            }
        }
        return file;
    }

    private Row readFirst(Path file, Schema schema) throws IOException {
        try (ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(new org.apache.hadoop.fs.Path(file.toUri()), schema)
                .withConf(configuration).build()) {
            return reader.read();
        }
    }
}
