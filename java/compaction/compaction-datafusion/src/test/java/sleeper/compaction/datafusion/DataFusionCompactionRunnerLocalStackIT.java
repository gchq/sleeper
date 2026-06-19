/*
 * Copyright 2022-2026 Crown Copyright
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.localstack.LocalStackContainer;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionTaskTestHelper;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
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
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.localstack.test.SleeperLocalStackContainer;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.parquet.row.RowReadSupport;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class DataFusionCompactionRunnerLocalStackIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    private final SketchesStore sketchesStore = new S3SketchesStore(s3Client, s3TransferManager);
    private final CompactionJobTracker jobTracker = new InMemoryCompactionJobTracker();
    @TempDir
    public Path tempDir;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Disabled("Expensive long running test only used for manual debugging")
    @Test
    void shouldRunManyCompactionsInSerialWithoutCrashing() throws Exception {
        // Given
        int iterations = Integer.getInteger("sleeper.test.compaction.stress.iterations", 1000);
        Schema schema = createSchemaWithKey("key", new StringType());
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
        Row row1 = new Row(Map.of("key", "row-1"));
        Row row2 = new Row(Map.of("key", "row-2"));
        String file1 = writeFileForPartition("root", List.of(row1));
        String file2 = writeFileForPartition("root", List.of(row2));

        try (FFIContext<DataFusionCompactionFunctions> context = FFIContext.getFFIContext(DataFusionCompactionFunctions.class)) {
            CompactionRunner runner = new DataFusionCompactionRunner(createAwsConfig(), new Configuration(), context);

            // When / Then — if the JVM crashes with SIGSEGV the test fails
            for (int i = 0; i < iterations; i++) {
                StateStore iterationStateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
                iterationStateStore.fixFileUpdateTime(null);
                update(iterationStateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
                update(iterationStateStore).addFile(FileReferenceFactory.from(iterationStateStore).partitionFile("root", file1, 1));
                update(iterationStateStore).addFile(FileReferenceFactory.from(iterationStateStore).partitionFile("root", file2, 1));
                String jobId = "stress-job-" + i;
                CompactionJob job = new CompactionJobFactory(instanceProperties, tableProperties)
                        .createCompactionJobWithFilenames(jobId, List.of(file1, file2), "root");
                update(iterationStateStore).assignJobId(jobId, "root", List.of(file1, file2));
                CompactionTaskTestHelper iterationHelper = new CompactionTaskTestHelper(
                        instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                        FixedStateStoreProvider.singleTable(tableProperties, iterationStateStore), jobTracker);
                iterationHelper.runTask(runner, null, List.of(job));
            }
        }
    }

    @Test
    void shouldRunCompactionJob() throws Exception {
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
        runTask(job);

        // Then
        assertThat(readDataFile(schema, job.getOutputFile()))
                .containsExactly(row1, row2);
        assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
    }

    @Test
    void shouldCompactEmptyFiles() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new StringType());
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
        String file1 = writeFileForPartition("root", List.of());
        String file2 = writeFileForPartition("root", List.of());
        CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

        // When
        runTask(job);

        // Then
        assertThat(readDataFile(schema, job.getOutputFile())).isEmpty();
        assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                .isEqualTo(SketchesDeciles.from(schema, List.of()));
        assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(0, 0));
    }

    @Test
    void shouldCallProgressCallbackWithCorrectRowCount() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new StringType());
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
        Row row1 = new Row(Map.of("key", "row-1"));
        Row row2 = new Row(Map.of("key", "row-2"));
        String file1 = writeFileForPartition("root", List.of(row1));
        String file2 = writeFileForPartition("root", List.of(row2));
        CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));
        AtomicLong callbackRowCount = new AtomicLong(-1);

        // When
        runTask(job, rowCount -> callbackRowCount.set(Math.toIntExact(rowCount)));

        // Then
        assertThat(readDataFile(schema, job.getOutputFile()))
                .containsExactly(row1, row2);
        assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                .isEqualTo(SketchesDeciles.from(schema, List.of(row1, row2)));
        assertThat(getRowsProcessed(job)).isEqualTo(new RowsProcessed(2, 2));
        assertThat(callbackRowCount.get()).isEqualTo(2);
    }

    private void runTask(CompactionJob job) throws Exception {
        runTask(job, null);
    }

    private void runTask(CompactionJob job, LongConsumer progressCallback) throws Exception {
        try (FFIContext<DataFusionCompactionFunctions> context = FFIContext.getFFIContext(DataFusionCompactionFunctions.class)) {
            CompactionRunner runner = new DataFusionCompactionRunner(createAwsConfig(), new Configuration(), context);
            compactionTaskTestHelper().runTask(runner, progressCallback, List.of(job));
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

    private static DataFusionAwsConfig createAwsConfig() {
        LocalStackContainer container = SleeperLocalStackContainer.INSTANCE;
        return DataFusionAwsConfig.overrideEndpoint(container.getEndpoint().toString());
    }

    private String writeFileForPartition(String partitionId, List<Row> rows) throws Exception {
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        String dataFile = buildPartitionFilePath(partitionId, UUID.randomUUID().toString() + ".parquet");
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(new org.apache.hadoop.fs.Path(dataFile), schema, hadoopConf)) {
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

    private String buildPartitionFilePath(String partitionId, String filename) {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, filename);
    }

    private List<Row> readDataFile(Schema schema, String filename) throws IOException {
        List<Row> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetReader.builder(new RowReadSupport(schema), new org.apache.hadoop.fs.Path(filename))
                        .withConf(hadoopConf).build())) {
            while (reader.hasNext()) {
                results.add(new Row(reader.next()));
            }
        }
        return results;
    }

    private Sketches readSketches(Schema schema, String filename) throws IOException {
        return sketchesStore.loadFileSketches(filename, schema);
    }
}
