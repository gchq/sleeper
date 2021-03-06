/*
 * Copyright 2022 Crown Copyright
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
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ParquetRecordReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldReadRecordsCorrectlyWithIntKey() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new IntType()));
        schema.setValueFields(new Field("column2", new StringType()));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
            .build();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", 5);
        map1.put("column2", "B");
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", 8);
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
        assertEquals(record1, readRecord1);
        assertEquals(2, readRecord1.getKeys().size());
        assertEquals(record2, readRecord2);
        assertEquals(2, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }

    @Test
    public void shouldReadRecordsCorrectlyWithByteArrayKey() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new ByteArrayType()));
        schema.setValueFields(new Field("column2", new StringType()));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
            .build();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
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
        //  - Replace byte array field in record1 with wrapped version so that equals works
        assertEquals(record1, readRecord1);
        assertEquals(2, readRecord1.getKeys().size());
        assertEquals(record2, readRecord2);
        assertEquals(2, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }

    @Test
    public void shouldReadRecordsCorrectlyWithByteArrayKeyAndMapValue() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new ByteArrayType()));
        schema.setValueFields(new Field("column2", new StringType()),
                new Field("column3", new MapType(new StringType(), new LongType())));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        Map<String, Long> map1Col3 = new HashMap<>();
        map1Col3.put("key1", 5L);
        map1Col3.put("key2", 50L);
        map1.put("column3", map1Col3);
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        Map<String, Long> map2Col3 = new HashMap<>();
        map2Col3.put("key1", 5L);
        map2.put("column3", map2Col3);
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertEquals(record1, readRecord1);
        assertEquals(3, readRecord1.getKeys().size());
        assertEquals(record2, readRecord2);
        assertEquals(3, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }

    @Test
    public void shouldReadRecordsCorrectlyWithByteArrayKeyAndEmptyMapValue() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new ByteArrayType()));
        schema.setValueFields(new Field("column2", new StringType()),
                new Field("column3", new MapType(new StringType(), new LongType())));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        Map<String, Long> map1Col3 = new HashMap<>();
        map1Col3.put("key1", 5L);
        map1Col3.put("key2", 50L);
        map1.put("column3", map1Col3);
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        Map<String, Long> map2Col3 = new HashMap<>();
        // map2Col3 is empty
        map2.put("column3", map2Col3);
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertEquals(record1, readRecord1);
        assertEquals(3, readRecord1.getKeys().size());
        assertEquals(record2, readRecord2);
        assertEquals(3, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }

    @Test
    public void shouldReadRecordsCorrectlyWithByteArrayKeyAndListValue() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new ByteArrayType()));
        schema.setValueFields(new Field("column2", new StringType()),
                new Field("column3", new ListType(new StringType())));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        List<String> list1 = new ArrayList<>();
        list1.add("element1");
        list1.add("element2");
        map1.put("column3", list1);
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        List<String> list2 = new ArrayList<>();
        list2.add("element1");
        map2.put("column3", list2);
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertEquals(record1, readRecord1);
        assertEquals(3, readRecord1.getKeys().size());
        assertEquals(record2, readRecord2);
        assertEquals(3, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }

    @Test
    public void shouldReadRecordsCorrectlyWithByteArrayKeyAndEmptyListValue() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new ByteArrayType()));
        schema.setValueFields(new Field("column2", new StringType()),
                new Field("column3", new ListType(new StringType())));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("column1", new byte[]{1, 2, 3});
        map1.put("column2", "B");
        List<String> list1 = new ArrayList<>();
        list1.add("element1");
        list1.add("element2");
        map1.put("column3", list1);
        Record record1 = new Record(map1);
        writer.write(record1);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("column1", new byte[]{4, 5, 6, 7});
        map2.put("column2", "D");
        List<String> list2 = new ArrayList<>();
        // list2 is empty
        map2.put("column3", list2);
        Record record2 = new Record(map2);
        writer.write(record2);
        writer.close();

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertEquals(record1, readRecord1);
        assertEquals(3, readRecord1.getKeys().size());
        assertEquals(record2, readRecord2);
        assertEquals(3, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }

    @Test
    public void shouldReadRecordsCorrectlyWithASubsetOfTheSchema() throws IOException {
        // Given
        Schema writeSchema = new Schema();
        writeSchema.setRowKeyFields(new Field("column1", new StringType()));
        writeSchema.setValueFields(new Field("column2", new StringType()));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(writeSchema), writeSchema)
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
        Schema readSchema = new Schema();
        readSchema.setRowKeyFields(new Field("column1", new StringType()));

        // When
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, readSchema).build();
        Record readRecord1 = new Record(reader.read());
        Record readRecord2 = new Record(reader.read());
        Record readRecord3 = reader.read();

        // Then
        assertEquals("A", readRecord1.get("column1"));
        assertEquals(1, readRecord1.getKeys().size());
        assertEquals("C", readRecord2.get("column1"));
        assertEquals(1, readRecord2.getKeys().size());
        assertNull(readRecord3);
    }
}
