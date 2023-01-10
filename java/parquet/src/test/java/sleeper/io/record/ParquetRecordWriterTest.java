/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.io.record;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetRecordWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldWriteRecordsCorrectlyForStringStringSchema() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new StringType()))
                .valueFields(new Field("column2", new StringType()))
                .build();
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
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
    public void shouldWriteRecordsCorrectlyForLongLongLongSchema() throws IOException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new LongType()))
                .sortKeyFields(new Field("column2", new LongType()))
                .valueFields(new Field("column3", new LongType()))
                .build();
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
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
    public void shouldWriteRecordsCorrectlyForByteArraySchema() throws IOException {
        // Given
        byte[] byteArray1 = new byte[]{1, 2, 3, 4, 5};
        byte[] byteArray2 = new byte[]{6, 7, 8, 9, 10};
        byte[] byteArray3 = new byte[]{11, 12, 13, 14, 15};
        byte[] byteArray4 = new byte[]{16, 17, 18, 19, 20, 20, 20, 20, 20};
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("column1", new ByteArrayType()))
                .valueFields(new Field("column2", new ByteArrayType()))
                .build();
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
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
}
