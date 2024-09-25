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
package sleeper.bulkimport.job.runner.dataframe;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
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
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

class FileWritingIteratorIT {

    @TempDir
    public java.nio.file.Path tempFolder;
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;

    @BeforeEach
    void setUp() {
        instanceProperties = createInstanceProperties();
        tableProperties = createTestTableProperties(instanceProperties, createSchema());
    }

    @Test
    void shouldReturnFalseForHasNextWithEmptyIterator() {
        // Given
        Iterator<Row> empty = new ArrayList<Row>().iterator();

        // When
        FileWritingIterator fileWritingIterator = createIteratorFrom(empty);

        // Then
        assertThat(fileWritingIterator).isExhausted();
    }

    @Test
    void shouldReturnTrueForHasNextWithPopulatedIterator() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")).iterator();

        // When
        FileWritingIterator fileWritingIterator = createIteratorFrom(input);

        // Then
        assertThat(fileWritingIterator).hasNext();
    }

    @Test
    void shouldGroupRecordsByFinalColumn() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")).iterator();

        // When
        FileWritingIterator fileWritingIterator = createIteratorFrom(input);

        // Then
        assertThat(fileWritingIterator).toIterable()
                .extracting(
                        row -> row.getString(0),
                        row -> row.getLong(2))
                .containsExactly(
                        tuple("a", 2L),
                        tuple("b", 2L));
    }

    @Test
    void shouldWriteAllRecordsToParquetFiles() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")).iterator();

        // When
        FileWritingIterator fileWritingIterator = createIteratorFrom(input);

        // Then
        assertThat(fileWritingIterator).toIterable()
                .extracting(row -> readRecords(row.getString(1)))
                .containsExactly(
                        List.of(
                                createRecord("a", 1, 2, "a"),
                                createRecord("b", 1, 2, "a")),
                        List.of(
                                createRecord("c", 1, 2, "b"),
                                createRecord("d", 1, 2, "b")));
    }

    @Test
    void shouldHandleExamplesWhereTheLastRecordBelongsToADifferentPartition() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b"),
                RowFactory.create("e", 1, 2, "c")).iterator();

        // When
        FileWritingIterator fileWritingIterator = createIteratorFrom(input);

        // Then
        assertThat(fileWritingIterator).toIterable()
                .extracting(row -> readRecords(row.getString(1)))
                .containsExactly(
                        List.of(
                                createRecord("a", 1, 2, "a"),
                                createRecord("b", 1, 2, "a")),
                        List.of(
                                createRecord("c", 1, 2, "b"),
                                createRecord("d", 1, 2, "b")),
                        List.of(
                                createRecord("e", 1, 2, "c")));
    }

    @Test
    void shouldGenerateCorrectParquetFilePaths() {
        // Given
        Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2, "a"),
                RowFactory.create("b", 1, 2, "a"),
                RowFactory.create("c", 1, 2, "b"),
                RowFactory.create("d", 1, 2, "b")).iterator();

        // When
        FileWritingIterator fileWritingIterator = createIteratorFrom(input, List.of("file1", "file2").iterator()::next);

        // Then
        assertThat(fileWritingIterator).toIterable()
                .extracting(row -> row.getString(1))
                .containsExactly(
                        "file://" + tempFolder + "/" + tableProperties.get(TABLE_ID) + "/data/partition_a/file1.parquet",
                        "file://" + tempFolder + "/" + tableProperties.get(TABLE_ID) + "/data/partition_b/file2.parquet");
    }

    private FileWritingIterator createIteratorFrom(Iterator<Row> input) {
        return createIteratorFrom(input, () -> UUID.randomUUID().toString());
    }

    private FileWritingIterator createIteratorFrom(Iterator<Row> input, Supplier<String> filenameSupplier) {
        return new FileWritingIterator(input, instanceProperties, tableProperties, new Configuration(), filenameSupplier);
    }

    private Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("int", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempFolder.toString());
        return instanceProperties;
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
}
