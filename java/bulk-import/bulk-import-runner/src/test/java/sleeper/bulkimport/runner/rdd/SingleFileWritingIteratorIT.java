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
package sleeper.bulkimport.runner.rdd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.partition.PartitionsFromSplitPoints;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

class SingleFileWritingIteratorIT {

    @TempDir
    public java.nio.file.Path tempFolder;

    private InstanceProperties instanceProperties;
    private final Schema schema = createSchema();
    private TableProperties tableProperties;

    @BeforeEach
    void setUp() {
        instanceProperties = createInstanceProperties();
        tableProperties = createTestTableProperties(instanceProperties, schema);
    }

    @Nested
    @DisplayName("Output a single file")
    class OutputSingleFile {

        private final Iterator<Row> input = List.of(
                RowFactory.create("a", 1, 2),
                RowFactory.create("b", 1, 2),
                RowFactory.create("c", 1, 2),
                RowFactory.create("d", 1, 2)).iterator();

        @Test
        void shouldWriteAllRowsToAParquetFile() {
            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRows(input);

            // Then
            assertThat(fileWritingIterator).toIterable()
                    .extracting(row -> readRows(readPathFromOutputFileMetadata(row)))
                    .containsExactly(
                            Arrays.asList(
                                    createRow("a", 1, 2),
                                    createRow("b", 1, 2),
                                    createRow("c", 1, 2),
                                    createRow("d", 1, 2)));
        }

        @Test
        void shouldOutputMetadataPointingToSingleFileInFolderForPartition() {
            // Given
            PartitionTree partitionTree = createPartitionsBuilder()
                    .singlePartition("test-partition")
                    .buildTree();

            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRowsWithPartitionsAndOutputFilename(
                    input, partitionTree, "test-file");

            // Then
            assertThat(fileWritingIterator).toIterable()
                    .containsExactly(RowFactory.create(
                            "test-partition",
                            "file://" + tempFolder + "/" + tableProperties.get(TABLE_ID) + "/data/partition_test-partition/test-file.parquet",
                            4));
        }
    }

    @Nested
    @DisplayName("Infer output partition")
    class InferOutputPartition {
        private SingleFileWritingIterator fileWritingIterator;

        @BeforeEach
        void setUp() {
            // Given
            Iterator<Row> input = List.of(
                    RowFactory.create("a", 1, 2),
                    RowFactory.create("b", 1, 2),
                    RowFactory.create("d", 1, 2),
                    RowFactory.create("e", 1, 2)).iterator();
            PartitionTree partitionTree = PartitionsBuilderSplitsFirst
                    .leavesWithSplits(schema, List.of("left", "right"), List.of("c"))
                    .parentJoining("root", "left", "right")
                    .buildTree();

            // When
            fileWritingIterator = createIteratorOverRowsWithPartitions(input, partitionTree);
        }

        @Test
        void shouldInferPartitionIdFromFirstRowWhenSomeRowsAreInDifferentPartitions() {
            // Then
            assertThat(fileWritingIterator).toIterable()
                    .extracting(SingleFileWritingIteratorIT.this::readPartitionIdFromOutputFileMetadata)
                    .containsExactly("left");
        }

        @Test
        void shouldAssumeAllRowsAreInTheSamePartition() {
            // Then
            assertThat(fileWritingIterator).toIterable()
                    .extracting(row -> readRows(readPathFromOutputFileMetadata(row)))
                    .containsExactly(
                            Arrays.asList(
                                    createRow("a", 1, 2),
                                    createRow("b", 1, 2),
                                    createRow("d", 1, 2),
                                    createRow("e", 1, 2)));
        }
    }

    @Nested
    @DisplayName("Behaves as an iterator")
    class BehavesAsAnIterator {

        @Test
        void shouldReturnTrueForHasNextWithPopulatedIterator() {
            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRows(
                    List.of(RowFactory.create("a", 1, 2)).iterator());

            // Then
            assertThat(fileWritingIterator).hasNext();
        }

        @Test
        void shouldReturnFalseForHasNextWithEmptyIterator() {
            // Given
            Iterator<Row> empty = Collections.emptyIterator();

            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRows(empty);

            // Then
            assertThat(fileWritingIterator).isExhausted();
        }
    }

    private SingleFileWritingIterator createIteratorOverRows(Iterator<Row> rows) {
        return createIteratorOverRowsWithPartitions(rows,
                PartitionsFromSplitPoints.treeFrom(schema, List.of("T")));
    }

    private SingleFileWritingIterator createIteratorOverRowsWithPartitions(
            Iterator<Row> rows, PartitionTree partitionTree) {
        return new SingleFileWritingIterator(rows,
                instanceProperties, tableProperties,
                new Configuration(), new LocalFileSystemSketchesStore(), partitionTree);
    }

    private SingleFileWritingIterator createIteratorOverRowsWithPartitionsAndOutputFilename(
            Iterator<Row> rows, PartitionTree partitionTree, String filename) {
        return new SingleFileWritingIterator(rows,
                instanceProperties, tableProperties,
                new Configuration(), partitionTree, new LocalFileSystemSketchesStore(), filename);
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempFolder.toString());
        return instanceProperties;
    }

    private static Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("int", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
    }

    private sleeper.core.row.Row createRow(Object... values) {
        return createRow(RowFactory.create(values), schema);
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
        try (ParquetReader<sleeper.core.row.Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(new Path(path), schema).build()) {
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

    private String readPartitionIdFromOutputFileMetadata(Row metadataRow) {
        return metadataRow.getString(0);
    }

    private String readPathFromOutputFileMetadata(Row metadataRow) {
        return metadataRow.getString(1);
    }

    private PartitionsBuilder createPartitionsBuilder() {
        return new PartitionsBuilder(schema);
    }
}
