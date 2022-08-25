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
import sleeper.core.schema.type.LongType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetReaderIteratorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldReturnCorrectIterator() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new LongType()));
        schema.setSortKeyFields(new Field("column2", new LongType()));
        schema.setValueFields(new Field("column3", new LongType()));
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();

        // When
        ParquetReaderIterator iterator = new ParquetReaderIterator(reader);

        // Then
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(record1);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(record2);
        assertThat(iterator.getNumberOfRecordsRead()).isEqualTo(2L);

        iterator.close();
    }

    @Test
    public void shouldReturnCorrectIteratorWhenNoRecordsInReader() throws IOException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("column1", new LongType()));
        schema.setSortKeyFields(new Field("column2", new LongType()));
        schema.setValueFields(new Field("column3", new LongType()));
        Path path = new Path(folder.newFolder().getAbsolutePath() + "/file.parquet");
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(path, SchemaConverter.getSchema(schema), schema)
                .build();
        writer.close();
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).build();

        // When
        ParquetReaderIterator iterator = new ParquetReaderIterator(reader);

        // Then
        assertThat(iterator.hasNext()).isFalse();
        assertThat(iterator.getNumberOfRecordsRead()).isZero();

        iterator.close();
    }
}