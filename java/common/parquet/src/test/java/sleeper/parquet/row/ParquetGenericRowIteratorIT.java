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
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    void shouldReadLogicalTypes() throws IOException {
        // Given
        MessageType parquetSchema = MessageTypeParser.parseMessageType("""
                message results {
                  required int32 decimalInt (DECIMAL(9,2));
                  required int64 decimalLong (DECIMAL(18,3));
                  required binary decimalBinary (DECIMAL(30,4));
                  required int32 date (DATE);
                  required int64 timestampMillis (TIMESTAMP(MILLIS,true));
                  required int64 timestampMicros (TIMESTAMP(MICROS,true));
                  required int64 timestampNanos (TIMESTAMP(NANOS,true));
                  required int64 timestampBeforeEpoch (TIMESTAMP(MICROS,true));
                  required int64 timestampLocal (TIMESTAMP(MILLIS,false));
                  required int96 timestampInt96;
                  required int32 timeMillis (TIME(MILLIS,true));
                  required int64 timeMicros (TIME(MICROS,true));
                  required fixed_len_byte_array(16) uuid (UUID);
                  required int32 unsignedInt (INTEGER(32,false));
                  required int64 unsignedLong (INTEGER(64,false));
                  required int32 signedShort (INTEGER(16,true));
                }
                """);
        UUID uuid = UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");
        Instant time = Instant.parse("2026-11-13T09:30:15.123Z");
        Path path = newPath();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(parquetSchema).build()) {
            Group group = new SimpleGroupFactory(parquetSchema).newGroup()
                    .append("decimalInt", 12345)
                    .append("decimalLong", -1234567L)
                    .append("decimalBinary", Binary.fromConstantByteArray(new BigInteger("123456789012345678901234567890").toByteArray()))
                    .append("date", (int) LocalDate.parse("2026-11-13").toEpochDay())
                    .append("timestampMillis", time.toEpochMilli())
                    .append("timestampMicros", time.toEpochMilli() * 1000 + 456)
                    .append("timestampNanos", time.toEpochMilli() * 1_000_000 + 456_789)
                    .append("timestampBeforeEpoch", -1L)
                    .append("timestampLocal", time.toEpochMilli())
                    .append("timestampInt96", new NanoTime(2_461_358, LocalTime.parse("09:30:15.123456789").toNanoOfDay()))
                    .append("timeMillis", (int) (LocalTime.parse("09:30:15.123").toNanoOfDay() / 1_000_000))
                    .append("timeMicros", LocalTime.parse("09:30:15.123456").toNanoOfDay() / 1000)
                    .append("uuid", Binary.fromConstantByteArray(ByteBuffer.allocate(16)
                            .putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array()))
                    .append("unsignedInt", -1)
                    .append("unsignedLong", -1L)
                    .append("signedShort", -5);
            writer.write(group);
        }

        // When
        List<Row> rows = readRows(path);

        // Then
        Row expected = new Row();
        expected.put("decimalInt", new BigDecimal("123.45"));
        expected.put("decimalLong", new BigDecimal("-1234.567"));
        expected.put("decimalBinary", new BigDecimal("12345678901234567890123456.7890"));
        expected.put("date", LocalDate.parse("2026-11-13"));
        expected.put("timestampMillis", Instant.parse("2026-11-13T09:30:15.123Z"));
        expected.put("timestampMicros", Instant.parse("2026-11-13T09:30:15.123456Z"));
        expected.put("timestampNanos", Instant.parse("2026-11-13T09:30:15.123456789Z"));
        expected.put("timestampBeforeEpoch", Instant.parse("1969-12-31T23:59:59.999999Z"));
        expected.put("timestampLocal", LocalDateTime.parse("2026-11-13T09:30:15.123"));
        expected.put("timestampInt96", Instant.parse("2026-11-13T09:30:15.123456789Z"));
        expected.put("timeMillis", LocalTime.parse("09:30:15.123"));
        expected.put("timeMicros", LocalTime.parse("09:30:15.123456"));
        expected.put("uuid", uuid);
        expected.put("unsignedInt", 4294967295L);
        expected.put("unsignedLong", new BigInteger("18446744073709551615"));
        expected.put("signedShort", -5);
        assertThat(rows).containsExactly(expected);
    }

    @Test
    void shouldReadListsNotWrittenBySleeper() throws IOException {
        // Given
        MessageType parquetSchema = MessageTypeParser.parseMessageType("""
                message results {
                  required group nullableElements (LIST) {
                    repeated group list {
                      optional binary element (STRING);
                    }
                  }
                  required group legacyList (LIST) {
                    repeated int32 array;
                  }
                  repeated int64 repeatedField;
                }
                """);
        Path path = newPath();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withType(parquetSchema).build()) {
            Group group = new SimpleGroupFactory(parquetSchema).newGroup();
            Group nullableElements = group.addGroup("nullableElements");
            nullableElements.addGroup("list").append("element", "a");
            nullableElements.addGroup("list");
            group.addGroup("legacyList").append("array", 1).append("array", 2);
            group.append("repeatedField", 5L).append("repeatedField", 6L);
            writer.write(group);
        }

        // When
        List<Row> rows = readRows(path);

        // Then
        Row expected = new Row();
        expected.put("nullableElements", Arrays.asList("a", null));
        expected.put("legacyList", List.of(1, 2));
        expected.put("repeatedField", List.of(5L, 6L));
        assertThat(rows).containsExactly(expected);
    }

    @Test
    void shouldReadFieldNamesComputedBySql() throws IOException {
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
