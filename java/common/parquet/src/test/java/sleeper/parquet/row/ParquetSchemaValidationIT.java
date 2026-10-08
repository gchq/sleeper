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
package sleeper.parquet.row;

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

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetSchemaValidationIT {
    @TempDir
    Path tempDir;

    private final Configuration configuration = new Configuration();
    private final Schema keySchema = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();

    @Test
    void shouldAcceptMatchingFileWithExtraColumns() throws IOException {
        // Given
        Path file = writeFile("required int64 extra; required int32 key;",
                group -> group.append("extra", 10L).append("key", 1));

        // When
        var problems = ParquetSchemaValidation.validateFile(keySchema, file.toString(), configuration);
        Row row = readFirst(file, keySchema);

        // Then
        assertThat(problems).isEmpty();
        assertThat(row).isEqualTo(new Row(Map.of("key", 1)));
    }

    @Test
    void shouldRejectNullableKeyEvenWhenValuesAreNotNull() throws IOException {
        // Given
        Path file = writeFile("optional int32 key;", group -> group.append("key", 1));

        // When
        var problems = ParquetSchemaValidation.validateFile(keySchema, file.toString(), configuration);

        // Then
        assertThat(problems).containsExactly("Field 'key' is nullable in the file but non-nullable in the table.");
        assertThatThrownBy(() -> readFirst(file, keySchema)).isInstanceOf(ParquetDecodingException.class);
    }

    @Test
    void shouldValidateSchemaOfEmptyFile() throws IOException {
        Path file = writeFile("optional int32 key;", null);

        assertThat(ParquetSchemaValidation.validateFile(keySchema, file.toString(), configuration))
                .containsExactly("Field 'key' is nullable in the file but non-nullable in the table.");
    }

    @Test
    void shouldReportAllMismatchedFields() throws IOException {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new LongType())).build();
        Path file = writeFile("required int64 key; required binary value;",
                group -> group.append("key", 1L).append("value", "x"));

        var problems = ParquetSchemaValidation.validateFile(schema, file.toString(), configuration);

        assertThat(problems).hasSize(2);
        assertThat(problems.get(0)).contains("Field 'key'", "incompatible");
        assertThat(problems.get(1)).contains("Field 'value'", "incompatible");
    }

    @Test
    void shouldRejectMissingNonNullableField() throws IOException {
        Path file = writeFile("required int32 other;", group -> group.append("other", 1));

        assertThat(ParquetSchemaValidation.validateFile(keySchema, file.toString(), configuration))
                .containsExactly("Missing non-nullable field 'key'.");
    }

    @Test
    void shouldAllowMissingNullableValue() throws IOException {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType(), true)).build();
        Path file = writeFile("required int32 key;", group -> group.append("key", 1));

        assertThat(ParquetSchemaValidation.validateFile(schema, file.toString(), configuration)).isEmpty();
        assertThat(readFirst(file, schema).get("value")).isNull();
    }

    @Test
    void shouldAllowRequiredFileColumnForNullableValue() throws IOException {
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType(), true)).build();
        Path file = writeFile("required binary value (UTF8); required int32 key;",
                group -> group.append("value", "x").append("key", 1));

        assertThat(ParquetSchemaValidation.validateFile(schema, file.toString(), configuration)).isEmpty();
        assertThat(readFirst(file, schema).get("value")).isEqualTo("x");
    }

    @Test
    void shouldRejectRepeatedScalar() throws IOException {
        Path file = writeFile("repeated int32 key;", group -> group.append("key", 1).append("key", 2));

        assertThat(ParquetSchemaValidation.validateFile(keySchema, file.toString(), configuration)).isNotEmpty();
        assertThatThrownBy(() -> readFirst(file, keySchema)).isInstanceOf(ParquetDecodingException.class);
    }

    @Test
    void shouldFailForNonParquetFile() throws IOException {
        Path file = Files.writeString(tempDir.resolve("not-parquet.txt"), "This is not a Parquet file.");

        assertThatThrownBy(() -> ParquetSchemaValidation.validateFile(keySchema, file.toString(), configuration))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldFailForMissingFile() {
        assertThatThrownBy(() -> ParquetSchemaValidation.validateFile(
                keySchema, tempDir.resolve("missing.parquet").toString(), configuration))
                .isInstanceOf(IOException.class);
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
        try (ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(
                new org.apache.hadoop.fs.Path(file.toUri()), schema).withConf(configuration).build()) {
            return reader.read();
        }
    }
}
