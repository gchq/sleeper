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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionRunner;
import sleeper.configuration.TableUtils;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestResult;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.io.parquet.record.RecordReadSupport;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class RustCompactionRunnerIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();
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
            Schema schema = schemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", "record-1"));
            Record record2 = new Record(Map.of("key", "record-2"));
            FileReference file1 = ingestRecordsGetFile(List.of(record1));
            FileReference file2 = ingestRecordsGetFile(List.of(record2));
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file1, file2), "root");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(file1.getFilename(), file2.getFilename()))));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
        }

        @Test
        void shouldMergeFilesWithLongKey() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", 1L));
            Record record2 = new Record(Map.of("key", 2L));
            FileReference file1 = ingestRecordsGetFile(List.of(record1));
            FileReference file2 = ingestRecordsGetFile(List.of(record2));
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file1, file2), "root");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(file1.getFilename(), file2.getFilename()))));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
        }

        @Test
        void shouldMergeFilesWithIntKey() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new IntType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", 1));
            Record record2 = new Record(Map.of("key", 2));
            FileReference file1 = ingestRecordsGetFile(List.of(record1));
            FileReference file2 = ingestRecordsGetFile(List.of(record2));
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file1, file2), "root");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(file1.getFilename(), file2.getFilename()))));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
        }

        @Test
        void shouldMergeFilesWithByteArrayKey() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new ByteArrayType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record1 = new Record(Map.of("key", new byte[]{1, 2}));
            Record record2 = new Record(Map.of("key", new byte[]{3, 4}));
            FileReference file1 = ingestRecordsGetFile(List.of(record1));
            FileReference file2 = ingestRecordsGetFile(List.of(record2));
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file1, file2), "root");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(file1.getFilename(), file2.getFilename()))));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isEqualTo(2);
            assertThat(summary.getRecordsWritten()).isEqualTo(2);
            assertThat(readDataFile(schema, job.getOutputFile()))
                    .containsExactly(record1, record2);
        }
    }

    @Nested
    @DisplayName("Handle empty files")
    class HandleEmptyFiles {

        @Test
        void shouldMergeEmptyAndNonEmptyFile() throws Exception {
            // Given
            Schema schema = schemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            Record record = new Record(Map.of("key", "test-value"));
            FileReference emptyFile = writeEmptyFileToPartition("root");
            FileReference nonEmptyFile = ingestRecordsGetFile(List.of(record));
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(emptyFile, nonEmptyFile), "root");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(emptyFile.getFilename(), nonEmptyFile.getFilename()))));

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
            Schema schema = schemaWithKey("key", new StringType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
            FileReference file1 = writeEmptyFileToPartition("root");
            FileReference file2 = writeEmptyFileToPartition("root");
            CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file1, file2), "root");
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(file1.getFilename(), file2.getFilename()))));

            // When
            RecordsProcessed summary = compact(job);

            // Then
            assertThat(summary.getRecordsRead()).isZero();
            assertThat(summary.getRecordsWritten()).isZero();
            assertThat(dataFileExists(job.getOutputFile())).isFalse();
        }
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private RecordsProcessed compact(CompactionJob job) throws Exception {
        CompactionRunner runner = new RustCompactionRunner(
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore));
        return runner.compact(job);
    }

    private FileReference ingestRecordsGetFile(List<Record> records) throws Exception {
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(tempDir, null).toString())
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .instanceProperties(instanceProperties)
                .build();
        IngestResult result = factory.ingestFromRecordIterator(tableProperties, records.iterator());
        List<FileReference> files = result.getFileReferenceList();
        if (files.size() != 1) {
            throw new IllegalStateException("Expected 1 file ingested, found: " + files);
        }
        return files.get(0);
    }

    private FileReference writeEmptyFileToPartition(String partitionId) throws Exception {
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        String dataFile = buildPartitionFilePath(partitionId, UUID.randomUUID().toString() + ".parquet");
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new org.apache.hadoop.fs.Path(dataFile), schema)) {
        }
        org.apache.hadoop.fs.Path sketchesPath = SketchesSerDeToS3.sketchesPathForDataFile(dataFile);
        new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPath, sketches, new Configuration());
        FileReference fileReference = FileReferenceFactory.from(stateStore).rootFile(dataFile, 0);
        stateStore.addFile(fileReference);
        return fileReference;
    }

    private String buildPartitionFilePath(String partitionId, String filename) {
        String prefix = TableUtils.buildDataFilePathPrefix(instanceProperties, tableProperties);
        return TableUtils.constructPartitionParquetFilePath(prefix, partitionId, filename);
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
}
