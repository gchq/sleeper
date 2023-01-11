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
package sleeper.bulkimport.job.runner.rdd;

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
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleFileWritingIteratorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("int", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
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

    private PartitionTree getPartitionTree() {
        return PartitionsFromSplitPoints.treeFrom(createSchema(), Arrays.asList("T"));
    }

    private Record createRecord(Object... values) {
        return createRecord(RowFactory.create(values), createSchema());
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

    private List<Record> readRecords(String path) {
        try (ParquetRecordReader reader = new ParquetRecordReader(new Path(path), createSchema())) {
            List<Record> records = new ArrayList<>();
            Record record = reader.read();
            while (null != record) {
                records.add(new Record(record));
                record = reader.read();
            }
            reader.close();
            return records;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void shouldReturnFalseForHasNextWithEmptyIterator() {
        // Given
        Iterator<Row> empty = new ArrayList<Row>().iterator();

        // When
        SingleFileWritingIterator fileWritingIterator = new SingleFileWritingIterator(empty,
                new InstanceProperties(), createTableProperties(),
                new Configuration(), getPartitionTree());

        // Then
        assertThat(fileWritingIterator).isExhausted();
    }

    @Test
    public void shouldReturnTrueForHasNextWithPopulatedIterator() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2),
                RowFactory.create("b", 1, 2),
                RowFactory.create("c", 1, 2),
                RowFactory.create("d", 1, 2)
        ).iterator();

        // When
        SingleFileWritingIterator fileWritingIterator = new SingleFileWritingIterator(input,
                new InstanceProperties(), createTableProperties(),
                new Configuration(), getPartitionTree());

        // Then
        assertThat(fileWritingIterator).hasNext();
    }

    @Test
    public void shouldWriteAllRecordsToAParquetFile() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2),
                RowFactory.create("b", 1, 2),
                RowFactory.create("c", 1, 2),
                RowFactory.create("d", 1, 2)
        ).iterator();

        // When
        SingleFileWritingIterator fileWritingIterator = new SingleFileWritingIterator(input,
                createInstanceProperties(), createTableProperties(),
                new Configuration(), getPartitionTree());

        // Then
        assertThat(fileWritingIterator).toIterable()
                .extracting(row -> readRecords(row.getString(1)))
                .containsExactly(
                        Arrays.asList(
                                createRecord("a", 1, 2),
                                createRecord("b", 1, 2),
                                createRecord("c", 1, 2),
                                createRecord("d", 1, 2)));
    }
}
