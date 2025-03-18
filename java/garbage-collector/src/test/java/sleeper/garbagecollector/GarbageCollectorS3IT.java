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
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.garbagecollector.GarbageCollector.deleteFilesAndSketches;

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
        createFileWithNoReferencesByCompaction(oldFile1, "new-file-1.parquet");
        createFileWithNoReferencesByCompaction("old-file-2.parquet", "new-file-2.parquet");

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOf(FailedGarbageCollectionException.class);
        assertThat(listObjectKeys(testBucket)).isEqualTo(Set.of(objectKey("new-file-1.parquet"), objectKey("new-file-2.parquet")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime,
                        List.of(activeReference("new-file-1.parquet"), activeReference("new-file-2.parquet")),
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
                fileWithNoReferences("/tmp/not-a-file.parquet")));
        createFileWithNoReferencesByCompaction("old-file.parquet", "new-file.parquet");

        // When
        collectGarbageAtTime(currentTime);

        // Then
        assertThat(listObjectKeys(testBucket)).isEqualTo(Set.of(objectKey("new-file.parquet")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeFilesReport(oldEnoughTime,
                        activeReference("new-file.parquet")));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, testBucket);
        return instanceProperties;
    }

    private FileReference activeReference(String filePath) {
        return fileFactory().rootFile(filePath, 100);
    }

    private void createFileWithNoReferencesByCompaction(
            String oldFilePath, String newFilePath) throws Exception {
        FileReference oldFile = createActiveFile("old-file.parquet");
        createFileWithNoReferencesByCompaction(oldFile, newFilePath);
    }

    private void createFileWithNoReferencesByCompaction(
            FileReference oldFile, String newFilePath) throws Exception {
        FileReference newFile = fileFactory().rootFile(newFilePath, 100);
        writeFile(newFilePath);
        update(stateStore).assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of(oldFile.getFilename()))));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of(oldFile.getFilename()), newFile)));
    }

    private FileReference createActiveFile(String filename) throws Exception {
        FileReference fileReference = fileFactory().rootFile(filename, 100L);
        update(stateStore).addFile(fileReference);
        writeFile(filename);
        return fileReference;
    }

    private void writeFile(String filename) {
        s3Client.putObject(testBucket, objectKey(filename), filename);
    }

    private String objectKey(String filename) {
        return TableFilePaths.buildObjectKeyInDataBucket(tableProperties).constructPartitionParquetFilePath("root", filename);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, partitions);
    }

    private void collectGarbageAtTime(Instant time) throws Exception {
        createGarbageCollector().runAtTime(time, List.of(tableProperties));
    }

    private GarbageCollector createGarbageCollector() {
        return new GarbageCollector(deleteFilesAndSketches(hadoopConf), instanceProperties,
                new FixedStateStoreProvider(tableProperties, stateStore),
                new SqsFifoStateStoreCommitRequestSender(instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(new FixedTablePropertiesProvider(tableProperties))));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
