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
package sleeper.compaction.datafusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.table.TableFilePaths;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.parquet.row.RowReadSupport;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class DataFusionCompactionRunnerIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    private final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();
    @TempDir
    public Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, createTempDirectory(tempDir, null).toString());
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
    }

    @Nested
    @DisplayName("Handle data types")
    class HandleDataTypes {

        @Test
        void shouldMergeFilesWithStringKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "row-1"));
            Row row2 = new Row(Map.of("key", "row-2"));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));
            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(2);
            assertThat(summary.getRowsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        }

        @Test
        void shouldMergeFilesWithLongKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", 1L));
            Row row2 = new Row(Map.of("key", 2L));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(2);
            assertThat(summary.getRowsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        }

        @Test
        void shouldMergeFilesWithIntKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", 1));
            Row row2 = new Row(Map.of("key", 2));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(2);
            assertThat(summary.getRowsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        }

        @Test
        void shouldMergeFilesWithByteArrayKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new ByteArrayType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", new byte[]{1, 2}));
            Row row2 = new Row(Map.of("key", new byte[]{3, 4}));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(2);
            assertThat(summary.getRowsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        }
    }

    @Nested
    @DisplayName("Handle empty files")
    class HandleEmptyFiles {

        @Test
        void shouldMergeEmptyAndNonEmptyFile() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row = new Row(Map.of("key", "test-value"));
            String emptyFile = writeFileForPartition("root", List.of());
            String nonEmptyFile = writeFileForPartition("root", List.of(row));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(emptyFile, nonEmptyFile));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(1);
            assertThat(summary.getRowsWritten()).isEqualTo(1);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(row);
        }

        @Test
        void shouldMergeTwoEmptyFiles() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            String file1 = writeFileForPartition("root", List.of());
            String file2 = writeFileForPartition("root", List.of());
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isZero();
            assertThat(summary.getRowsWritten()).isZero();
            assertThat(dataFileExists(job.getOutputFile())).isFalse();
        }
    }

    @Nested
    @DisplayName("Write sketches")
    class WriteSketches {

        @Test
        void shouldWriteSketchWhenMergingFiles() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "row-1"));
            Row row2 = new Row(Map.of("key", "row-2"));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            compact(job);

            // Then
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        }
    }

    @Nested
    @DisplayName("Handle aggregation")
    class HandleAggregation {
        @Test
        void shouldMergeAndAggregate() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("sort", new StringType()))
                    .valueFields(new Field("value", new LongType()), new Field("map_value2", new MapType(new StringType(), new LongType())))
                    .build();
            tableProperties.setSchema(schema);
            tableProperties.set(TableProperty.ITERATOR_CLASS_NAME, DataEngine.AGGREGATION_ITERATOR_NAME);
            tableProperties.set(TableProperty.ITERATOR_CONFIG, "sort;,sum(value),map_sum(map_value2)");
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "sort", "b", "value", 1L, "map_value2", Map.of("map_key1", 1L, "map_key2", 3L)));
            Row row2 = new Row(Map.of("key", "a", "sort", "b", "value", 2L, "map_value2", Map.of("map_key1", 3L, "map_key2", 4L)));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            Row output1 = new Row(Map.of("key", "a", "sort", "b", "value", 3L, "map_value2", Map.of("map_key1", 4L, "map_key2", 7L)));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(2);
            assertThat(summary.getRowsWritten()).isEqualTo(1);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(output1);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(output1)));
        }

        @Test
        void shouldMergeAndFilterAgeOff() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("timestamp", new LongType()))
                    .valueFields(new Field("value", new LongType()))
                    .build();
            tableProperties.setSchema(schema);
            tableProperties.set(TableProperty.ITERATOR_CLASS_NAME, DataEngine.AGGREGATION_ITERATOR_NAME);
            tableProperties.set(TableProperty.ITERATOR_CONFIG, "timestamp;ageoff=timestamp,10,sum(value)");
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "timestamp", 999999999999999L, "value", 1L));
            Row row2 = new Row(Map.of("key", "a", "timestamp", 1L, "value", 2L));
            Row row3 = new Row(Map.of("key", "a", "timestamp", 999999999999999L, "value", 3L));

            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2, row3));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            Row output1 = new Row(Map.of("key", "a", "timestamp", 999999999999999L, "value", 4L));

            // When
            RowsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRowsRead()).isEqualTo(2);
            assertThat(summary.getRowsWritten()).isEqualTo(1);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(output1);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(output1)));
        }
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private RowsProcessed compact(CompactionJob job) throws Exception {
        CompactionRunner runner = new DataFusionCompactionRunner();
        return runner.compact(job, tableProperties, stateStore.getPartition(job.getPartitionId()).getRegion());
    }

    private String writeFileForPartition(String partitionId, List<Row> rows) throws Exception {
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        String dataFile = buildPartitionFilePath(partitionId, UUID.randomUUID().toString() + ".parquet");
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(new org.apache.hadoop.fs.Path(dataFile), schema)) {
            for (Row row : rows) {
                writer.write(row);
                sketches.update(row);
            }
        }
        sketchesStore.saveFileSketches(dataFile, schema, sketches);
        return dataFile;
    }

    private CompactionJob createCompactionForPartition(String jobId, String partitionId, List<String> filenames) {
        return compactionFactory().createCompactionJobWithFilenames(jobId, filenames, partitionId);
    }

    private String buildPartitionFilePath(String partitionId, String filename) {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, filename);
    }

    private List<Row> readDataFile(Schema schema, String filename) throws IOException {
        List<Row> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetReader.builder(new RowReadSupport(schema), new org.apache.hadoop.fs.Path(filename)).build())) {
            while (reader.hasNext()) {
                results.add(new Row(reader.next()));
            }
        }
        return results;
    }

    private boolean dataFileExists(String filename) throws IOException {
        return FileSystem.getLocal(new Configuration())
                .exists(new org.apache.hadoop.fs.Path(filename));
    }

    private Sketches readSketches(Schema schema, String filename) throws IOException {
        return sketchesStore.loadFileSketches(filename, schema);
    }
}
