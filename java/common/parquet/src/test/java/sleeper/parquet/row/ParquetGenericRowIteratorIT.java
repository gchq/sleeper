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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

class ParquetGenericRowIteratorIT {

    @TempDir
    public java.nio.file.Path folder;

    @Test
    void shouldReadRowsWrittenWithEveryTypeOfSleeperField() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("int", new IntType()))
                .sortKeyFields(new Field("long", new LongType()))
                .valueFields(
                        new Field("string", new StringType()),
                        new Field("bytes", new ByteArrayType()),
                        new Field("stringToLong", new MapType(new StringType(), new LongType())),
                        new Field("strings", new ListType(new StringType())),
                        new Field("ints", new ListType(new IntType())),
                        new Field("longs", new ListType(new LongType())))
                .build();
        Row row1 = new Row();
        row1.put("int", 5);
        row1.put("long", 50L);
        row1.put("string", "B");
        row1.put("bytes", new byte[]{1, 2});
        row1.put("stringToLong", Map.of("a", 1L, "b", 2L));
        row1.put("strings", List.of("x", "y"));
        row1.put("ints", List.of(1, 2, 3));
        row1.put("longs", List.of(4L));
        Row row2 = new Row();
        row2.put("int", 8);
        row2.put("long", 80L);
        row2.put("string", "");
        row2.put("bytes", new byte[]{});
        row2.put("stringToLong", Map.of());
        row2.put("strings", List.of());
        row2.put("ints", List.of());
        row2.put("longs", List.of());
        Path path = writeRows(schema, row1, row2);

        // When / Then
        assertThat(readRows(path)).containsExactly(row1, row2);
    }

    @Test
    void shouldReadMapsWithKeysThatAreNotStrings() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(
                        new Field("intToString", new MapType(new IntType(), new StringType())),
                        new Field("longToLong", new MapType(new LongType(), new LongType())),
                        new Field("nullableIntToInt", new MapType(new IntType(), new IntType()), true))
                .build();
        Row row1 = new Row();
        row1.put("key", "A");
        row1.put("intToString", Map.of(1, "one", 2, "two"));
        row1.put("longToLong", Map.of(10L, 100L));
        row1.put("nullableIntToInt", Map.of(3, 4));
        Row row2 = new Row();
        row2.put("key", "B");
        row2.put("intToString", Map.of());
        row2.put("longToLong", Map.of());
        row2.put("nullableIntToInt", null);
        Path path = writeRows(schema, row1, row2);

        // When / Then
        assertThat(readRows(path)).containsExactly(row1, row2);
    }

    @Test
    void shouldReadMapAndListHoldingByteArrays() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(
                        new Field("bytesToBytes", new MapType(new ByteArrayType(), new ByteArrayType())),
                        new Field("stringToBytes", new MapType(new StringType(), new ByteArrayType())),
                        new Field("byteArrays", new ListType(new ByteArrayType())))
                .build();
        Row row = new Row();
        row.put("key", "A");
        row.put("bytesToBytes", Map.of(new byte[]{3}, new byte[]{4}));
        row.put("stringToBytes", Map.of("k", new byte[]{8, 9}));
        row.put("byteArrays", List.of(new byte[]{5}, new byte[]{6, 7}));
        Path path = writeRows(schema, row);

        // When
        Row found = readRows(path).get(0);

        // Then
        Map<?, ?> bytesToBytes = (Map<?, ?>) found.get("bytesToBytes");
        assertThat(bytesToBytes).hasSize(1);
        assertThat(bytesToBytes.keySet().iterator().next()).isEqualTo(new byte[]{3});
        assertThat(bytesToBytes.values().iterator().next()).isEqualTo(new byte[]{4});
        assertThat(((Map<?, ?>) found.get("stringToBytes")).get("k")).isEqualTo(new byte[]{8, 9});
        List<?> byteArrays = (List<?>) found.get("byteArrays");
        assertThat(byteArrays).hasSize(2);
        assertThat(byteArrays.get(0)).isEqualTo(new byte[]{5});
        assertThat(byteArrays.get(1)).isEqualTo(new byte[]{6, 7});
    }

    @Test
    void shouldReadJavaTypesHeldInSleeperRows() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(
                        new Field("bytes", new ByteArrayType()),
                        new Field("map", new MapType(new StringType(), new StringType())),
                        new Field("list", new ListType(new StringType())))
                .build();
        Row row = new Row();
        row.put("key", "A");
        row.put("bytes", new byte[]{1});
        row.put("map", Map.of("k", "v"));
        row.put("list", List.of("e"));
        Path path = writeRows(schema, row);

        // When
        Row found = readRows(path).get(0);

        // Then
        assertThat(found.get("key")).isExactlyInstanceOf(String.class);
        assertThat(found.get("bytes")).isExactlyInstanceOf(byte[].class);
        assertThat((Map<?, ?>) found.get("map")).isExactlyInstanceOf(HashMap.class)
                .allSatisfy((key, value) -> {
                    assertThat(key).isExactlyInstanceOf(String.class);
                    assertThat(value).isExactlyInstanceOf(String.class);
                });
        assertThat((List<?>) found.get("list")).isExactlyInstanceOf(ArrayList.class)
                .allSatisfy(element -> assertThat(element).isExactlyInstanceOf(String.class));
    }

    @Test
    void shouldReadNullValuesOfNullableFields() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(
                        new Field("string", new StringType(), true),
                        new Field("bytes", new ByteArrayType(), true),
                        new Field("int", new IntType(), true),
                        new Field("long", new LongType(), true),
                        new Field("map", new MapType(new StringType(), new StringType()), true),
                        new Field("list", new ListType(new StringType()), true))
                .build();
        Row nulls = new Row();
        nulls.put("key", "A");
        nulls.put("string", null);
        nulls.put("bytes", null);
        nulls.put("int", null);
        nulls.put("long", null);
        nulls.put("map", null);
        nulls.put("list", null);
        Row values = new Row();
        values.put("key", "B");
        values.put("string", "s");
        values.put("bytes", new byte[]{1});
        values.put("int", 1);
        values.put("long", 2L);
        values.put("map", Map.of("k", "v"));
        values.put("list", List.of("e"));
        Path path = writeRows(schema, nulls, values);

        // When / Then
        assertThat(readRows(path)).containsExactly(nulls, values);
    }

    @Test
    void shouldReadFileWithNoRows() throws IOException {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        Path path = writeRows(schema);

        // When / Then
        assertThat(readRows(path)).isEmpty();
    }

    @Test
    void shouldReadTypesThatAreNotSleeperTypes() throws IOException {
        // Given
        // - These types might be returned by a Sleeper query with SQL applied
        MessageType parquetSchema = MessageTypeParser.parseMessageType("""
                message results {
                  required binary name (STRING);
                  required double average;
                  required float ratio;
                  required boolean flag;
                  optional double missing;
                  required group nested {
                    required int64 count;
                    required binary label (STRING);
                  }
                }
                """);
        Path path = newPath();
        SimpleGroupFactory factory = new SimpleGroupFactory(parquetSchema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(parquetSchema).build()) {
            Group group = factory.newGroup()
                    .append("name", "A")
                    .append("average", 1.5d)
                    .append("ratio", 0.25f)
                    .append("flag", true);
            group.addGroup("nested").append("count", 3L).append("label", "L");
            writer.write(group);
        }

        // When
        List<Row> rows = readRows(path);

        // Then
        Row expected = new Row();
        expected.put("name", "A");
        expected.put("average", 1.5d);
        expected.put("ratio", 0.25f);
        expected.put("flag", true);
        expected.put("missing", null);
        expected.put("nested", Map.of("count", 3L, "label", "L"));
        assertThat(rows).containsExactly(expected);
    }

    @Test
    void shouldReadFieldNamesThatAreNotValidInAvro() throws IOException {
        // Given
        // - These field names might be returned when SQL is applied to a query
        MessageType parquetSchema = Types.buildMessage()
                .required(PrimitiveTypeName.INT64).named("count(*)")
                .required(PrimitiveTypeName.INT64).named("query_results.value + Int64(1)")
                .named("results");
        Path path = newPath();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(parquetSchema).build()) {
            writer.write(new SimpleGroupFactory(parquetSchema).newGroup()
                    .append("count(*)", 3L)
                    .append("query_results.value + Int64(1)", 4L));
        }

        // When / Then
        assertThat(readRows(path)).containsExactly(
                new Row(Map.of("count(*)", 3L, "query_results.value + Int64(1)", 4L)));
    }

    private Path newPath() throws IOException {
        return new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
    }

    private Path writeRows(Schema schema, Row... rows) throws IOException {
        Path path = newPath();
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema)) {
            for (Row row : rows) {
                writer.write(row);
            }
        }
        return path;
    }

    private List<Row> readRows(Path path) throws IOException {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = ParquetRowReaderFactory.parquetGenericRowIterator(path, new Configuration())) {
            iterator.forEachRemaining(rows::add);
        }
        return rows;
    }
}
