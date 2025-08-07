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
package sleeper.bulkimport.runner.dataframe;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.sketches.store.LocalFileSystemSketchesStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

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
    void shouldGroupRowsByFinalColumn() {
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
    void shouldWriteAllRowsToParquetFiles() {
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
                .extracting(row -> readRows(row.getString(1)))
                .containsExactly(
                        List.of(
                                createRow("a", 1, 2, "a"),
                                createRow("b", 1, 2, "a")),
                        List.of(
                                createRow("c", 1, 2, "b"),
                                createRow("d", 1, 2, "b")));
    }

    @Test
    void shouldHandleExamplesWhereTheLastRowBelongsToADifferentPartition() {
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
                .extracting(row -> readRows(row.getString(1)))
                .containsExactly(
                        List.of(
                                createRow("a", 1, 2, "a"),
                                createRow("b", 1, 2, "a")),
                        List.of(
                                createRow("c", 1, 2, "b"),
                                createRow("d", 1, 2, "b")),
                        List.of(
                                createRow("e", 1, 2, "c")));
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
        return new FileWritingIterator(input, instanceProperties, tableProperties, new Configuration(),
                new LocalFileSystemSketchesStore(), filenameSupplier);
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

    private sleeper.core.row.Row createRow(Object... values) {
        return createRow(RowFactory.create(values), createSchema());
    }

    private sleeper.core.row.Row createRow(Row row, Schema schema) {
        sleeper.core.row.Row outRow = new sleeper.core.row.Row();
        int i = 0;
        for (Field field : schema.getAllFields()) {
            outRow.put(field.getName(), row.get(i));
            i++;
        }
        return outRow;
    }

    private List<sleeper.core.row.Row> readRows(String path) {
        try (ParquetReader<sleeper.core.row.Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(new Path(path), createSchema()).build()) {
            List<sleeper.core.row.Row> rows = new ArrayList<>();
            sleeper.core.row.Row row = reader.read();
            while (null != row) {
                rows.add(new sleeper.core.row.Row(row));
                row = reader.read();
            }
            reader.close();
            return rows;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
