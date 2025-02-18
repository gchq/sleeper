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
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.garbagecollector.GarbageCollector.deleteFileAndSketches;

public class GarbageCollectorS3IT extends LocalStackTestBase {

    private static final Schema TEST_SCHEMA = getSchema();
    private final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitions);
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
    void shouldContinueCollectingFilesIfTryingToDeleteFileThrowsIOException() throws Exception {
        // Given
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        // Create a FileReference referencing a file in a bucket that does not exist
        FileReference oldFile1 = factory.rootFile("s3a://not-a-bucket/old-file-1.parquet", 100L);
        stateStore.addFile(oldFile1);
        // Perform a compaction on an existing file to create a readyForGC file
        s3Client.putObject(testBucket, "old-file-2.parquet", "abc");
        s3Client.putObject(testBucket, "new-file-2.parquet", "def");
        FileReference oldFile2 = factory.rootFile("s3a://" + testBucket + "/old-file-2.parquet", 100L);
        FileReference newFile2 = factory.rootFile("s3a://" + testBucket + "/new-file-2.parquet", 100L);
        stateStore.addFile(oldFile2);
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root",
                        List.of(oldFile1.getFilename(), oldFile2.getFilename()))));
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of(oldFile1.getFilename(), oldFile2.getFilename()), newFile2)));

        // When
        GarbageCollector collector = createGarbageCollector(instanceProperties, tableProperties, stateStore);

        // And / Then
        assertThatThrownBy(() -> collector.runAtTime(currentTime, List.of(tableProperties)))
                .isInstanceOf(FailedGarbageCollectionException.class);
        assertThat(s3Client.doesObjectExist(testBucket, "old-file-2.parquet")).isFalse();
        assertThat(s3Client.doesObjectExist(testBucket, "new-file-2.parquet")).isTrue();
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime,
                        List.of(newFile2),
                        List.of(oldFile1.getFilename())));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, testBucket);
        return instanceProperties;
    }

    private GarbageCollector createGarbageCollector(InstanceProperties instanceProperties, TableProperties tableProperties, StateStore stateStore) {
        return new GarbageCollector(deleteFileAndSketches(hadoopConf), instanceProperties,
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
