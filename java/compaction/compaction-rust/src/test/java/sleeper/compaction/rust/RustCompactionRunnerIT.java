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
package sleeper.compaction.rust;

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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.table.TableFilePaths;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.parquet.record.RecordReadSupport;
import sleeper.sketchesv2.Sketches;
import sleeper.sketchesv2.store.LocalFileSystemSketchesStore;
import sleeper.sketchesv2.store.SketchesStore;
import sleeper.sketchesv2.testutils.SketchesDeciles;

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

public class RustCompactionRunnerIT {

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
            Record record1 = new Record(Map.of("key", "record-1"));
            Record record2 = new Record(Map.of("key", "record-2"));
            String file1 = writeFileForPartition("root", List.of(record1));
            String file2 = writeFileForPartition("root", List.of(record2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));
            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(record1, record2)));
        }

        @Test
        void shouldMergeFilesWithLongKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", 1L));
            Record record2 = new Record(Map.of("key", 2L));
            String file1 = writeFileForPartition("root", List.of(record1));
            String file2 = writeFileForPartition("root", List.of(record2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(record1, record2)));
        }

        @Test
        void shouldMergeFilesWithIntKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", 1));
            Record record2 = new Record(Map.of("key", 2));
            String file1 = writeFileForPartition("root", List.of(record1));
            String file2 = writeFileForPartition("root", List.of(record2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(record1, record2)));
        }

        @Test
        void shouldMergeFilesWithByteArrayKey() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key", new ByteArrayType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", new byte[]{1, 2}));
            Record record2 = new Record(Map.of("key", new byte[]{3, 4}));
            String file1 = writeFileForPartition("root", List.of(record1));
            String file2 = writeFileForPartition("root", List.of(record2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(record1, record2)));
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
            Record record = new Record(Map.of("key", "test-value"));
            String emptyFile = writeFileForPartition("root", List.of());
            String nonEmptyFile = writeFileForPartition("root", List.of(record));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(emptyFile, nonEmptyFile));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(1);
            assertThat(summary.getRecordsWritten()).isEqualTo(1);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record);
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
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isZero();
            assertThat(summary.getRecordsWritten()).isZero();
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
            Record record1 = new Record(Map.of("key", "record-1"));
            Record record2 = new Record(Map.of("key", "record-2"));
            String file1 = writeFileForPartition("root", List.of(record1));
            String file2 = writeFileForPartition("root", List.of(record2));
            CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

            // When
            compact(job);

            // Then
            assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                    .isEqualTo(SketchesDeciles.from(schema, List.of(record1, record2)));
        }
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private RecordsProcessed compact(CompactionJob job) throws Exception {
        CompactionRunner runner = new RustCompactionRunner();
        return runner.compact(job, tableProperties, stateStore.getPartition(job.getPartitionId()));
    }

    private String writeFileForPartition(String partitionId, List<Record> records) throws Exception {
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        String dataFile = buildPartitionFilePath(partitionId, UUID.randomUUID().toString() + ".parquet");
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new org.apache.hadoop.fs.Path(dataFile), schema)) {
            for (Record record : records) {
                writer.write(record);
                sketches.update(record);
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

    private List<Record> readDataFile(Schema schema, String filename) throws IOException {
        List<Record> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetReader.builder(new RecordReadSupport(schema), new org.apache.hadoop.fs.Path(filename)).build())) {
            while (reader.hasNext()) {
                results.add(new Record(reader.next()));
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
