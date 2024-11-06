/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.parquet.record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;

class ParquetRecordWriterFactoryIT {

    @TempDir
    public java.nio.file.Path folder;

    @Test
    void shouldWriteRecordsCorrectlyForStringStringSchema() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new StringType()))
                .valueFields(new Field("column2", new StringType()))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", "A");
        map1.put("column2", "B");
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", "C");
        map2.put("column2", "D");
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertThat(readRecord1.get("column1")).isEqualTo("A");
        assertThat(readRecord1.get("column2")).isEqualTo("B");
        assertThat(readRecord2.get("column1")).isEqualTo("C");
        assertThat(readRecord2.get("column2")).isEqualTo("D");
        assertThat(readRecord3).isNull();
    }

    @Test
    void shouldWriteRecordsCorrectlyForLongLongLongSchema() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new LongType()))
                .sortKeyFields(new Field("column2", new LongType()))
                .valueFields(new Field("column3", new LongType()))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", 1L);
        map1.put("column2", 2L);
        map1.put("column3", 3L);
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", 4L);
        map2.put("column2", 5L);
        map2.put("column3", 6L);
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertThat(readRecord1.get("column1")).isEqualTo(1L);
        assertThat(readRecord1.get("column2")).isEqualTo(2L);
        assertThat(readRecord1.get("column3")).isEqualTo(3L);
        assertThat(readRecord2.get("column1")).isEqualTo(4L);
        assertThat(readRecord2.get("column2")).isEqualTo(5L);
        assertThat(readRecord2.get("column3")).isEqualTo(6L);
        assertThat(readRecord3).isNull();
    }

    @Test
    void shouldWriteRecordsCorrectlyForByteArraySchema() throws IOException {
        // Given
        byte[] byteArray1 = new byte[]{1, 2, 3, 4, 5};
        byte[] byteArray2 = new byte[]{6, 7, 8, 9, 10};
        byte[] byteArray3 = new byte[]{11, 12, 13, 14, 15};
        byte[] byteArray4 = new byte[]{16, 17, 18, 19, 20, 20, 20, 20, 20};
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(new Field("column2", new ByteArrayType()))
                .build();
        Path path = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, schema);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", byteArray1);
        map1.put("column2", byteArray2);
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", byteArray3);
        map2.put("column2", byteArray4);
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertThat((byte[]) readRecord1.get("column1")).containsExactly(byteArray1);
        assertThat((byte[]) readRecord1.get("column2")).containsExactly(byteArray2);
        assertThat((byte[]) readRecord2.get("column1")).containsExactly(byteArray3);
        assertThat((byte[]) readRecord2.get("column2")).containsExactly(byteArray4);
        assertThat(readRecord3).isNull();
    }

    @Test
    void shouldRespectDictionaryEncodingOption() throws IOException {
        // Given
        byte[] byteArray1 = new byte[]{1, 2, 3, 4, 5};
        byte[] byteArray2 = new byte[]{6, 7, 8, 9, 10};
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .sortKeyFields(new Field("column2", new StringType()))
                .valueFields(new Field("column3", new ByteArrayType()))
                .build();
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", byteArray1);
        map1.put("column2", "ABC");
        map1.put("column3", byteArray2);
        Record record = new Record(map1);
        // When/Then - Dictionary encoding on
        tableProperties.set(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS, "true");
        tableProperties.set(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS, "true");
        tableProperties.set(DICTIONARY_ENCODING_FOR_VALUE_FIELDS, "true");
        Path path1 = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        writeParquetFile(path1, tableProperties, record);
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path1, new Configuration()))) {
            List<ColumnChunkMetaData> columns = reader.getRowGroups().get(0).getColumns();
            assertThat(columns.get(0).getEncodings().stream().map(Encoding::usesDictionary)).containsExactlyInAnyOrder(true, false);
            assertThat(columns.get(1).getEncodings().stream().map(Encoding::usesDictionary)).containsExactlyInAnyOrder(true, false);
            assertThat(columns.get(2).getEncodings().stream().map(Encoding::usesDictionary)).containsExactlyInAnyOrder(true, false);
        }

        // When/Then - Dictionary encoding off
        tableProperties.set(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS, "false");
        tableProperties.set(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS, "false");
        tableProperties.set(DICTIONARY_ENCODING_FOR_VALUE_FIELDS, "false");
        Path path2 = new Path(createTempDirectory(folder, null).toString() + "/file.parquet");
        writeParquetFile(path2, tableProperties, record);
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path2, new Configuration()))) {
            List<ColumnChunkMetaData> columns = reader.getRowGroups().get(0).getColumns();
            assertThat(columns.get(0).getEncodings().stream().map(Encoding::usesDictionary)).containsOnly(false);
            assertThat(columns.get(1).getEncodings().stream().map(Encoding::usesDictionary)).containsOnly(false);
            assertThat(columns.get(2).getEncodings().stream().map(Encoding::usesDictionary)).containsOnly(false);
        }
    }

    private void writeParquetFile(Path path, TableProperties tableProperties, Record record) throws IOException {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, tableProperties, new Configuration());
        writeRecordNTimes(writer, record, 10_000);
        writer.close();
    }

    private void writeRecordNTimes(ParquetWriter<Record> writer, Record record, int numTimes) throws IOException {
        for (int i = 0; i < numTimes; i++) {
            writer.write(record);
        }
    }
}
