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
package sleeper.ingest.runner.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.runner.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.runner.testutils.RecordGenerator;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.runner.testutils.IngestCoordinatorTestHelper.standardIngestCoordinatorBuilder;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.ingest.runner.testutils.ResultVerifier.readRecordsFromPartitionDataFile;

public class IngestCoordinatorUsingDirectWriteBackedByArrayListIT extends LocalStackTestBase {

    @TempDir
    public Path temporaryFolder;

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
    private final RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
            new LongType(),
            LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
    private final PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
            .rootFirst("root")
            .splitToNewChildren("root", "left", "right", 0L)
            .buildTree();
    private final SketchesStore sketchesStore = new S3SketchesStore(s3Client, s3TransferManager);
    private StateStore stateStore;

    @BeforeEach
    public void before() {
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
        stateStore = createStateStore(recordListAndSchema.sleeperSchema);
        update(stateStore).initialise(tree.getAllPartitions());
        stateStore.fixFileUpdateTime(stateStoreUpdateTime);
    }

    private StateStore createStateStore(Schema schema) {
        tableProperties.setSchema(schema);
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        // Given
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();

        // When
        ingestRecords(5, 1000L, ingestLocalWorkingDirectory, Stream.of("leftFile", "rightFile"));

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference leftFile = fileReferenceFactory.partitionFile("left", "s3a://" + dataBucketName + "/data/partition_left/leftFile.parquet", 100);
        FileReference rightFile = fileReferenceFactory.partitionFile("right", "s3a://" + dataBucketName + "/data/partition_right/rightFile.parquet", 100);
        List<Record> leftRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, leftFile, hadoopConf);
        List<Record> rightRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, rightFile, hadoopConf);
        List<Record> allRecords = Stream.of(leftRecords, rightRecords).flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(leftRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(LongStream.range(-100, 0).boxed().toArray());
        assertThat(rightRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(LongStream.range(0, 100).boxed().toArray());
        assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalStore() throws Exception {
        // Given
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Stream<String> fileNames = IntStream.iterate(0, i -> i + 1).mapToObj(i -> "file" + i);

        // When
        ingestRecords(5, 10L, ingestLocalWorkingDirectory, fileNames);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, stateStoreUpdateTime);
        FileReference firstLeftFile = fileReferenceFactory.partitionFile("left", "s3a://" + dataBucketName + "/data/partition_left/file0.parquet", 5);
        FileReference firstRightFile = fileReferenceFactory.partitionFile("right", "s3a://" + dataBucketName + "/data/partition_right/file1.parquet", 5);
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        List<Record> firstLeftFileRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, firstLeftFile, hadoopConf);
        List<Record> firstRightFileRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, firstRightFile, hadoopConf);

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).hasSize(40)
                .contains(firstLeftFile, firstRightFile);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(firstLeftFileRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(-90L, -79L, -68L, -50L, -2L);
        assertThat(firstRightFileRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(12L, 14L, 41L, 47L, 83L);
        assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
    }

    private void ingestRecords(
            int maxNoOfRecordsInMemory,
            long maxNoOfRecordsInLocalStore,
            String ingestLocalWorkingDirectory,
            Stream<String> fileNames) throws IteratorCreationException, IOException {
        ParquetConfiguration parquetConfiguration = parquetConfiguration(
                recordListAndSchema.sleeperSchema, hadoopConf);
        try (IngestCoordinator<Record> ingestCoordinator = standardIngestCoordinatorBuilder(
                stateStore, recordListAndSchema.sleeperSchema,
                ArrayListRecordBatchFactory.builder()
                        .parquetConfiguration(parquetConfiguration)
                        .localWorkingDirectory(ingestLocalWorkingDirectory)
                        .maxNoOfRecordsInMemory(maxNoOfRecordsInMemory)
                        .maxNoOfRecordsInLocalStore(maxNoOfRecordsInLocalStore)
                        .buildAcceptingRecords(),
                DirectPartitionFileWriterFactory.builder()
                        .parquetConfiguration(parquetConfiguration)
                        .filePaths(TableFilePaths.fromPrefix("s3a://" + dataBucketName))
                        .sketchesStore(sketchesStore)
                        .fileNameGenerator(fileNames.iterator()::next)
                        .build())
                .ingestFileWritingStrategy(tableProperties.getEnumValue(INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.class))
                .build()) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }
}
