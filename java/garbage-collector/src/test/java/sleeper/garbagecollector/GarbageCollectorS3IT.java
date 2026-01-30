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

package sleeper.garbagecollector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.table.TableFilePaths;
import sleeper.garbagecollector.FailedGarbageCollectionException.FileFailure;
import sleeper.garbagecollector.GarbageCollector.DeleteFiles;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FilesReportTestHelper.referencedAndUnreferencedFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.referencedFilesReport;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.util.ThreadSleepTestHelper.refuseWaits;

public class GarbageCollectorS3IT extends LocalStackTestBase {

    private static final Schema TEST_SCHEMA = getSchema();
    private final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    private final String testBucket = UUID.randomUUID().toString();
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());

    @BeforeEach
    void setUp() {
        createBucket(testBucket);
    }

    StateStore setupStateStoreAndFixTime(Instant fixedTime) {
        stateStore.fixFileUpdateTime(fixedTime);
        return stateStore;
    }

    @Test
    void shouldContinueCollectingFilesIfBucketDoesNotExist() throws Exception {
        // Given
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        // Create a FileReference referencing a file in a bucket that does not exist
        FileReference oldFile1 = FileReferenceFactory.from(partitions).rootFile("s3a://not-a-bucket/old-file-1.parquet", 100L);
        update(stateStore).addFile(oldFile1);
        // Perform a compaction on an existing file to create a readyForGC file
        createFileWithNoReferencesByCompaction(oldFile1, "new-file-1");
        createFileWithNoReferencesByCompaction("old-file-2", "new-file-2");

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOfSatisfying(FailedGarbageCollectionException.class,
                        e -> assertThat(e.getTableFailures()).singleElement().satisfies(failure -> {
                            assertThat(failure.table()).isEqualTo(tableProperties.getStatus());
                            assertThat(failure.streamFailures())
                                    .singleElement()
                                    .isInstanceOf(NoSuchBucketException.class);
                            assertThat(failure.fileFailures())
                                    .flatExtracting(FileFailure::filenames)
                                    .containsExactly("s3a://not-a-bucket/old-file-1.parquet");
                        }));
        assertThat(listObjectKeys(testBucket)).isEqualTo(Set.of(
                dataFileObjectKey("new-file-1"),
                sketchesFileObjectKey("new-file-1"),
                dataFileObjectKey("new-file-2"),
                sketchesFileObjectKey("new-file-2")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(referencedAndUnreferencedFilesReport(oldEnoughTime,
                        List.of(fileReference("new-file-1"), fileReference("new-file-2")),
                        List.of(oldFile1.getFilename())));
    }

    @Test
    void shouldContinueCollectingFilesIfFileDoesNotExist() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        update(stateStore).addFilesWithReferences(List.of(
                fileWithNoReferences(dataFileFullFilename("not-a-file"))));
        createFileWithNoReferencesByCompaction("old-file", "new-file");

        // When
        collectGarbageAtTime(currentTime);

        // Then
        assertThat(listObjectKeys(testBucket)).isEqualTo(Set.of(
                dataFileObjectKey("new-file"),
                sketchesFileObjectKey("new-file")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(referencedFilesReport(oldEnoughTime,
                        fileReference("new-file")));
    }

    @Test
    void shouldDeleteMoreThanS3BatchSize() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction("old-file1", "new-file1");
        createFileWithNoReferencesByCompaction("old-file2", "new-file2");
        createFileWithNoReferencesByCompaction("old-file3", "new-file3");

        // When
        collectGarbageAtTimeWithS3BatchSize(currentTime, 2);

        //Then
        assertThat(listObjectKeys(testBucket)).isEqualTo(Set.of(
                dataFileObjectKey("new-file1"),
                sketchesFileObjectKey("new-file1"),
                dataFileObjectKey("new-file2"),
                sketchesFileObjectKey("new-file2"),
                dataFileObjectKey("new-file3"),
                sketchesFileObjectKey("new-file3")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(referencedFilesReport(oldEnoughTime,
                        fileReference("new-file1"),
                        fileReference("new-file2"),
                        fileReference("new-file3")));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, testBucket);
        return instanceProperties;
    }

    private FileReference fileReference(String filePath) {
        return fileFactory().rootFile(filePath, 100);
    }

    private void createFileWithNoReferencesByCompaction(
            String oldFilePath, String newFilePath) throws Exception {
        FileReference oldFile = createReferencedFile(oldFilePath);
        createFileWithNoReferencesByCompaction(oldFile, newFilePath);
    }

    private void createFileWithNoReferencesByCompaction(
            FileReference oldFile, String newFilePath) throws Exception {
        FileReference newFile = fileFactory().rootFile(newFilePath, 100);
        writeFileAndSketches(newFilePath);
        update(stateStore).assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of(oldFile.getFilename()))));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of(oldFile.getFilename()), newFile)));
    }

    private FileReference createReferencedFile(String filename) throws Exception {
        FileReference fileReference = fileFactory().rootFile(filename, 100L);
        update(stateStore).addFile(fileReference);
        writeFileAndSketches(filename);
        return fileReference;
    }

    private void writeFileAndSketches(String filename) {
        putObject(testBucket, dataFileObjectKey(filename), filename);
        putObject(testBucket, sketchesFileObjectKey(filename), filename);
    }

    private String dataFileFullFilename(String filename) {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties).constructPartitionParquetFilePath("root", filename);
    }

    private String dataFileObjectKey(String filename) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructPartitionParquetFilePath("root", filename);
    }

    private String sketchesFileObjectKey(String filename) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructQuantileSketchesFilePath("root", filename);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, partitions);
    }

    private void collectGarbageAtTime(Instant time) throws Exception {
        collectGarbageAtTimeWithS3BatchSize(time, 1000);
    }

    private void collectGarbageAtTimeWithS3BatchSize(Instant time, int s3BatchSize) throws Exception {
        createGarbageCollector(new S3DeleteFiles(s3Client, s3BatchSize, refuseWaits()))
                .runAtTime(time, List.of(tableProperties));
    }

    private GarbageCollector createGarbageCollector(DeleteFiles deleteFiles) {
        return new GarbageCollector(deleteFiles, instanceProperties,
                FixedStateStoreProvider.singleTable(tableProperties, stateStore),
                new SqsFifoStateStoreCommitRequestSender(instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(new FixedTablePropertiesProvider(tableProperties))));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
