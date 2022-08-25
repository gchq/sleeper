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
package sleeper.bulkimport.job.runner;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class FileWritingIteratorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Schema createSchema() {
        Schema schema = new Schema();

        schema.setRowKeyFields(new Field("key", new StringType()));
        schema.setSortKeyFields(new Field("int", new IntType()));
        schema.setValueFields(new Field("value", new IntType()));

        return schema;
    }

    private TableProperties createTableProperties() {
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(createSchema());
        try {
            tableProperties.set(TableProperty.DATA_BUCKET, tempFolder.newFolder().getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp folder for test", e);
        }

        return tableProperties;
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(UserDefinedInstanceProperty.FILE_SYSTEM, "file://");
        return instanceProperties;
    }

    @Test
    public void shouldReturnFalseForHasNextWithEmptyIterator() {
        // Given
        Iterator<Row> empty = new ArrayList<Row>().iterator();

        // When
        FileWritingIterator fileWritingIterator = new FileWritingIterator(empty,
                new InstanceProperties(), createTableProperties(),
                new Configuration());

        // Then
        assertThat(fileWritingIterator.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnTrueForHasNextWithPopulatedIterator() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")
        ).iterator();

        // When
        FileWritingIterator fileWritingIterator = new FileWritingIterator(input,
                new InstanceProperties(), createTableProperties(),
                new Configuration());

        // Then
        assertThat(fileWritingIterator.hasNext()).isTrue();
    }

    @Test
    public void shouldGroupRecordsByFinalColumn() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")
        ).iterator();

        // When
        FileWritingIterator fileWritingIterator = new FileWritingIterator(input,
                createInstanceProperties(), createTableProperties(),
                new Configuration());

        List<Row> rows = new ArrayList<>();

        while (fileWritingIterator.hasNext()) {
            rows.add(fileWritingIterator.next());
        }

        // Then
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(0)).isEqualTo("a");
        assertThat(rows.get(0).getLong(2)).isEqualTo(2L);
        assertThat(rows.get(1).getString(0)).isEqualTo("b");
        assertThat(rows.get(1).getLong(2)).isEqualTo(2L);
    }

    @Test
    public void shouldWriteAllRecordsToParquetFiles() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")
        ).iterator();

        // When
        FileWritingIterator fileWritingIterator = new FileWritingIterator(input,
                createInstanceProperties(), createTableProperties(),
                new Configuration());

        List<Row> rows = new ArrayList<>();

        while (fileWritingIterator.hasNext()) {
            rows.add(fileWritingIterator.next());
        }

        checkData(rows.get(0).getString(1), Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a")));
        checkData(rows.get(1).getString(1), Lists.newArrayList(
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")));
    }

    @Test
    public void shouldHandleExamplesWhereTheLastRecordBelongsToADifferentPartition() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b"),
                RowFactory.create("e", 1, 2, "c")
        ).iterator();

        // When
        FileWritingIterator fileWritingIterator = new FileWritingIterator(input,
                createInstanceProperties(), createTableProperties(),
                new Configuration());

        List<Row> rows = new ArrayList<>();

        while (fileWritingIterator.hasNext()) {
            rows.add(fileWritingIterator.next());
        }

        checkData(rows.get(0).getString(1), Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a")));
        checkData(rows.get(1).getString(1), Lists.newArrayList(
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")));
        checkData(rows.get(2).getString(1), Lists.newArrayList(
                RowFactory.create("e", 1, 2, "c")));
    }

    private Record createRecord(Row row, Schema schema) {
        Record record = new Record();
        int i = 0;
        for (Field field : schema.getAllFields()) {
            record.put(field.getName(), row.get(i));
            i++;
        }
        return record;
    }

    private void checkData(String path, List<Row> expectedRows) {
        try {
            ParquetRecordReader reader = new ParquetRecordReader(new Path(path), createSchema());
            List<Record> expected = expectedRows.stream()
                    .map(row -> this.createRecord(row, createSchema()))
                    .collect(Collectors.toList());
            List<Record> records = new ArrayList<>();
            Record record = reader.read();
            while (null != record) {
                records.add(new Record(record));
                record = reader.read();
            }
            reader.close();

            assertThat(records).isEqualTo(expected);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}