/*
 * Copyright 2022-2025 Crown Copyright
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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

class ParquetRowReaderIT {

    @TempDir
    public java.nio.file.Path folder;

    @Test
    void shouldReadRowsCorrectlyWithIntKey() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new IntType()))
                .valueFields(new Field("column2", new StringType()))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", 5);
        map1.put("column2", "B");
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", 8);
        map2.put("column2", "D");
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        assertThat(readRow1).isEqualTo(row1);
        assertThat(readRow1.getKeys()).hasSize(2);
        assertThat(readRow2).isEqualTo(row2);
        assertThat(readRow2.getKeys()).hasSize(2);
        assertThat(readRow3).isNull();
    }

    @Test
    void shouldReadRowsCorrectlyWithByteArrayKey() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(new Field("column2", new StringType()))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        //  - Replace byte array field in row1 with wrapped version so that equals works
        assertThat(readRow1).isEqualTo(row1);
        assertThat(readRow1.getKeys()).hasSize(2);
        assertThat(readRow2).isEqualTo(row2);
        assertThat(readRow2.getKeys()).hasSize(2);
        assertThat(readRow3).isNull();
    }

    @Test
    void shouldReadRowsCorrectlyWithByteArrayKeyAndMapValue() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(
                        new Field("column2", new StringType()),
                        new Field("column3", new MapType(new StringType(), new LongType())))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        Map<String, Long> map1Col3 = new HashMap<>();
        map1Col3.put("key1", 5L);
        map1Col3.put("key2", 50L);
        map1.put("column3", map1Col3);
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        Map<String, Long> map2Col3 = new HashMap<>();
        map2Col3.put("key1", 5L);
        map2.put("column3", map2Col3);
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        assertThat(readRow1).isEqualTo(row1);
        assertThat(readRow1.getKeys()).hasSize(3);
        assertThat(readRow2).isEqualTo(row2);
        assertThat(readRow2.getKeys()).hasSize(3);
        assertThat(readRow3).isNull();
    }

    @Test
    void shouldReadRowsCorrectlyWithByteArrayKeyAndEmptyMapValue() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(
                        new Field("column2", new StringType()),
                        new Field("column3", new MapType(new StringType(), new LongType())))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        Map<String, Long> map1Col3 = new HashMap<>();
        map1Col3.put("key1", 5L);
        map1Col3.put("key2", 50L);
        map1.put("column3", map1Col3);
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        Map<String, Long> map2Col3 = new HashMap<>();
        // map2Col3 is empty
        map2.put("column3", map2Col3);
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        assertThat(readRow1).isEqualTo(row1);
        assertThat(readRow1.getKeys()).hasSize(3);
        assertThat(readRow2).isEqualTo(row2);
        assertThat(readRow2.getKeys()).hasSize(3);
        assertThat(readRow3).isNull();
    }

    @Test
    void shouldReadRowsCorrectlyWithByteArrayKeyAndListValue() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(
                        new Field("column2", new StringType()),
                        new Field("column3", new ListType(new StringType())))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        List<String> list1 = new ArrayList<>();
        list1.add("element1");
        list1.add("element2");
        map1.put("column3", list1);
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        List<String> list2 = new ArrayList<>();
        list2.add("element1");
        map2.put("column3", list2);
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        assertThat(readRow1).isEqualTo(row1);
        assertThat(readRow1.getKeys()).hasSize(3);
        assertThat(readRow2).isEqualTo(row2);
        assertThat(readRow2.getKeys()).hasSize(3);
        assertThat(readRow3).isNull();
    }

    @Test
    void shouldReadRowsCorrectlyWithByteArrayKeyAndEmptyListValue() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(
                        new Field("column2", new StringType()),
                        new Field("column3", new ListType(new StringType())))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        List<String> list1 = new ArrayList<>();
        list1.add("element1");
        list1.add("element2");
        map1.put("column3", list1);
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        List<String> list2 = new ArrayList<>();
        // list2 is empty
        map2.put("column3", list2);
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        assertThat(readRow1).isEqualTo(row1);
        assertThat(readRow1.getKeys()).hasSize(3);
        assertThat(readRow2).isEqualTo(row2);
        assertThat(readRow2.getKeys()).hasSize(3);
        assertThat(readRow3).isNull();
    }

    @Test
    void shouldReadRowsCorrectlyWithASubsetOfTheSchema() throws IOException {
        // Given
        Schema writeSchema = Schema.builder()
                .rowKeyFields(new Field("column1", new StringType()))
                .valueFields(new Field("column2", new StringType()))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, writeSchema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", "A");
        map1.put("column2", "B");
        Row row1 = new Row(map1);
        writer.write(row1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", "C");
        map2.put("column2", "D");
        Row row2 = new Row(map2);
        writer.write(row2);
        writer.close();
        Schema readSchema = Schema.builder().rowKeyFields(new Field("column1", new StringType())).build();

        // When
        ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, readSchema).build();
        Row readRow1 = new Row(reader.read());
        Row readRow2 = new Row(reader.read());
        Row readRow3 = reader.read();

        // Then
        assertThat(readRow1.get("column1")).isEqualTo("A");
        assertThat(readRow1.getKeys()).hasSize(1);
        assertThat(readRow2.get("column1")).isEqualTo("C");
        assertThat(readRow2.getKeys()).hasSize(1);
        assertThat(readRow3).isNull();
    }
}
