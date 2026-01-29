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
import sleeper.compaction.core.task.CompactionTaskTestHelper;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.table.TableFilePaths;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.CompactionJobTrackerTestHelper;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
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
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithMultipleKeys;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class DataFusionCompactionRunnerIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    private final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();
    private final CompactionJobTracker jobTracker = new InMemoryCompactionJobTracker();
    @TempDir
    public Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, createTempDirectory(tempDir, null).toString());
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 1);
        stateStore.fixFileUpdateTime(null);
    }

    @Test
    void shouldMergeFilesWithStringAndLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithMultipleKeys("foo1", new StringType(), "bar1", new LongType());
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "L", "R", 0, "h")
                .splitToNewChildrenOnDimension("R", "RL", "RR", 1, 10L)
                .buildTree());
        Row rowExcludedByFirstKey = new Row(Map.of("foo1", "a-row", "bar1", 20L));
        Row rowExcludedBySecondKey = new Row(Map.of("foo1", "some-row", "bar1", 5L));
        Row row1 = new Row(Map.of("foo1", "row-1", "bar1", 11L));
        Row row2 = new Row(Map.of("foo1", "row-2", "bar1", 12L));
        String file1 = writeFileForPartition("RR", List.of(row1, rowExcludedByFirstKey));
        String file2 = writeFileForPartition("RR", List.of(row2, rowExcludedBySecondKey));
        CompactionJob job = createCompactionForPartition("test-job", "RR", List.of(file1, file2));

        // When
        runTask(job);

        // Then
        assertThat(readDataFile(job.getOutputFile()))
                .containsExactly(row1, row2);
        assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        assertThat(stateStore.getFileReferences())
                .containsExactly(outputFileReference(job, 2));
        assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
    }

    @Nested
    @DisplayName("Handle data types")
    class HandleDataTypes {

        @Test
        void shouldMergeFilesWithStringKey() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "row-1"));
            Row row2 = new Row(Map.of("key", "row-2"));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of(row1, row2)));
        }

        @Test
        void shouldMergeFilesWithLongKey() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new LongType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", 1L));
            Row row2 = new Row(Map.of("key", 2L));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of(row1, row2)));
        }

        @Test
        void shouldMergeFilesWithIntKey() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new IntType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", 1));
            Row row2 = new Row(Map.of("key", 2));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of(row1, row2)));
        }

        @Test
        void shouldMergeFilesWithByteArrayKey() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new ByteArrayType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", new byte[]{1, 2}));
            Row row2 = new Row(Map.of("key", new byte[]{3, 4}));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of(row1, row2)));
        }
    }

    @Nested
    @DisplayName("Handle empty files")
    class HandleEmptyFiles {

        @Test
        void shouldMergeEmptyAndNonEmptyFile() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row = new Row(Map.of("key", "test-value"));
            String emptyFile = writeFileForPartition("root", List.of());
            String nonEmptyFile = writeFileForPartition("root", List.of(row));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(emptyFile, nonEmptyFile));

            // When
            runTask(job);

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(1, 1));
            assertThat(readDataFile(job.getOutputFile()))
                    .containsExactly(row);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of(row)));
        }

        @Test
        void shouldMergeTwoEmptyFiles() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new StringType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            String file1 = writeFileForPartition("root", List.of());
            String file2 = writeFileForPartition("root", List.of());
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // The should_merge_empty_files test in rust/sleeper_core/tests/compaction_test.rs asserts that no result file
            // is written when empty files are compacted. This differs here as it appears DataFusion will not always
            // write an empty results file. See comment in that file. We ensure a result file is always written on the Java
            // side in DataFusionCompactionRunner.

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(0, 0));
            assertThat(readDataFile(job.getOutputFile())).isEmpty();
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of()));
            assertThat(stateStore.getFileReferences())
                    .containsExactly(outputFileReference(job, 0));
        }
    }

    @Nested
    @DisplayName("Handle aggregation and filtering")
    class HandleAggregationAndFiltering {
        @Test
        void shouldAggregate() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("sort", new StringType()))
                    .valueFields(new Field("value", new LongType()), new Field("map_value2", new MapType(new StringType(), new LongType())))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "sum(value), map_sum(map_value2)");
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "sort", "b", "value", 1L, "map_value2", Map.of("map_key1", 1L, "map_key2", 3L)));
            Row row2 = new Row(Map.of("key", "a", "sort", "b", "value", 2L, "map_value2", Map.of("map_key1", 3L, "map_key2", 4L)));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            List<Row> expected = List.of(
                    new Row(Map.of(
                            "key", "a",
                            "sort", "b",
                            "value", 3L,
                            "map_value2", Map.of(
                                    "map_key1", 4L,
                                    "map_key2", 7L))));
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 1));
            assertThat(readDataFile(job.getOutputFile()))
                    .isEqualTo(expected);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, expected));
        }

        @Test
        void shouldFilter() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("timestamp", new LongType()))
                    .valueFields(new Field("value", new LongType()))
                    .build());
            tableProperties.set(FILTERING_CONFIG, "ageOff(timestamp,10)");
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "timestamp", 999999999999998L, "value", 1L));
            Row row2 = new Row(Map.of("key", "a", "timestamp", 1L, "value", 2L));
            Row row3 = new Row(Map.of("key", "a", "timestamp", 999999999999999L, "value", 3L));

            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2, row3));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            List<Row> expected = List.of(
                    new Row(Map.of(
                            "key", "a",
                            "timestamp", 999999999999998L,
                            "value", 1L)),
                    new Row(Map.of(
                            "key", "a",
                            "timestamp", 999999999999999L,
                            "value", 3L)));
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .isEqualTo(expected);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, expected));
        }

        @Test
        void shouldFilterAndAggregate() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("timestamp", new LongType()))
                    .valueFields(new Field("value", new LongType()))
                    .build());
            tableProperties.set(FILTERING_CONFIG, "ageOff(timestamp,10)");
            tableProperties.set(AGGREGATION_CONFIG, "sum(value)");
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "timestamp", 999999999999999L, "value", 1L));
            Row row2 = new Row(Map.of("key", "a", "timestamp", 1L, "value", 2L));
            Row row3 = new Row(Map.of("key", "a", "timestamp", 999999999999999L, "value", 3L));

            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2, row3));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            List<Row> expected = List.of(
                    new Row(Map.of(
                            "key", "a",
                            "timestamp", 999999999999999L,
                            "value", 4L)));
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 1));
            assertThat(readDataFile(job.getOutputFile()))
                    .isEqualTo(expected);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, expected));
        }
    }

    @Nested
    @DisplayName("Handle unusual field names")
    class UnusualFieldNames {

        @Test
        void shouldCompactFileWithUnusualKeyNames() throws Exception {
            // Given
            tableProperties.setSchema(createSchemaWithMultipleKeys("SoMeKeY", new StringType(), "Other@Key", new LongType()));
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("SoMeKeY", "row-1", "Other@Key", 123L));
            Row row2 = new Row(Map.of("SoMeKeY", "row-2", "Other@Key", 456L));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .containsExactly(row1, row2);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, List.of(row1, row2)));
        }

        @Test
        void shouldApplyFilterOnUnusualFieldName() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("TimeStamp", new LongType()))
                    .valueFields(new Field("value", new LongType()))
                    .build());
            tableProperties.set(FILTERING_CONFIG, "ageOff(TimeStamp,10)");
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "TimeStamp", 999999999999998L, "value", 1L));
            Row row2 = new Row(Map.of("key", "a", "TimeStamp", 1L, "value", 2L));
            Row row3 = new Row(Map.of("key", "a", "TimeStamp", 999999999999999L, "value", 3L));

            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2, row3));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            List<Row> expected = List.of(
                    new Row(Map.of(
                            "key", "a",
                            "TimeStamp", 999999999999998L,
                            "value", 1L)),
                    new Row(Map.of(
                            "key", "a",
                            "TimeStamp", 999999999999999L,
                            "value", 3L)));
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
            assertThat(readDataFile(job.getOutputFile()))
                    .isEqualTo(expected);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, expected));
        }

        @Test
        void shouldAggregateWithUnusualValueFieldName() throws Exception {
            // Given
            tableProperties.setSchema(Schema.builder()
                    .rowKeyFields(new Field("key", new StringType()))
                    .sortKeyFields(new Field("sort", new StringType()))
                    .valueFields(new Field("LongValue", new LongType()), new Field("MapValue", new MapType(new StringType(), new LongType())))
                    .build());
            tableProperties.set(AGGREGATION_CONFIG, "sum(LongValue), map_sum(MapValue)");
            update(stateStore).initialise(new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
            Row row1 = new Row(Map.of("key", "a", "sort", "b", "LongValue", 1L, "MapValue", Map.of("map_key1", 1L, "map_key2", 3L)));
            Row row2 = new Row(Map.of("key", "a", "sort", "b", "LongValue", 2L, "MapValue", Map.of("map_key1", 3L, "map_key2", 4L)));
            String file1 = writeFileForPartition("root", List.of(row1));
            String file2 = writeFileForPartition("root", List.of(row2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            runTask(job);

            // Then
            List<Row> expected = List.of(
                    new Row(Map.of(
                            "key", "a",
                            "sort", "b",
                            "LongValue", 3L,
                            "MapValue", Map.of(
                                    "map_key1", 4L,
                                    "map_key2", 7L))));
            assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 1));
            assertThat(readDataFile(job.getOutputFile()))
                    .isEqualTo(expected);
            assertThat(SketchesDeciles.from(readSketches(job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(tableProperties, expected));
        }
    }

    private void runTask(CompactionJob job) throws Exception {
        try (FFIContext<DataFusionCompactionFunctions> context = FFIContext.getFFIContext(DataFusionCompactionFunctions.class)) {
            CompactionRunner runner = new DataFusionCompactionRunner(
                    // DataFusion spends time trying to auth with AWS unless you override it
                    DataFusionAwsConfig.overrideEndpoint("dummy"),
                    new Configuration(), context);
            compactionTaskTestHelper().runTask(runner, List.of(job));
        }
    }

    private CompactionTaskTestHelper compactionTaskTestHelper() {
        return new CompactionTaskTestHelper(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                FixedStateStoreProvider.singleTable(tableProperties, stateStore), jobTracker);
    }

    private RowsProcessed getRowsProcessed(CompactionJob job) {
        return CompactionJobTrackerTestHelper.getRowsProcessed(jobTracker, job.getId());
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
        update(stateStore).addFile(FileReferenceFactory.from(stateStore).partitionFile(partitionId, dataFile, rows.size()));
        sketchesStore.saveFileSketches(dataFile, schema, sketches);
        return dataFile;
    }

    private CompactionJob createCompactionForPartition(String jobId, String partitionId, List<String> filenames) {
        CompactionJob job = compactionFactory().createCompactionJobWithFilenames(jobId, filenames, partitionId);
        update(stateStore).assignJobId(jobId, partitionId, filenames);
        return job;
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected FileReference outputFileReference(CompactionJob job, long numberOfRows) {
        return FileReferenceFactory.from(stateStore).partitionFile(job.getPartitionId(), job.getOutputFile(), numberOfRows);
    }

    private String buildPartitionFilePath(String partitionId, String filename) {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, filename);
    }

    private List<Row> readDataFile(String filename) throws IOException {
        List<Row> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetReader.builder(new RowReadSupport(tableProperties.getSchema()), new org.apache.hadoop.fs.Path(filename)).build())) {
            while (reader.hasNext()) {
                results.add(new Row(reader.next()));
            }
        }
        return results;
    }

    private Sketches readSketches(String filename) throws IOException {
        return sketchesStore.loadFileSketches(filename, tableProperties.getSchema());
    }
}
