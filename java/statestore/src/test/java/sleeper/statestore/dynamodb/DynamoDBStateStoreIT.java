/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DynamoDBStateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, DYNAMO_PORT, AmazonDynamoDBClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions,
                                             int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, String.valueOf(garbageCollectorDelayBeforeDeletionInMinutes));
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDBClient);
        DynamoDBStateStore stateStore = dynamoDBStateStoreCreator.create(tableProperties);
        stateStore.initialise(partitions);
        return stateStore;
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions) throws StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private DynamoDBStateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct(), garbageCollectorDelayBeforeDeletionInMinutes);
    }

    private DynamoDBStateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, 0);
    }

    private Schema schemaWithSingleRowKeyType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private Schema schemaWithTwoRowKeyTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field("key1", type1), new Field("key2", type2)).build();
    }

    private Schema schemaWithKeyAndValueWithTypes(PrimitiveType keyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", keyType))
                .valueFields(new Field("value", valueType))
                .build();
    }

    @Test
    public void shouldReturnCorrectFileInfoForLongRowKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(1L));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(10L));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoForByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(new byte[]{1}))
                .maxRowKey(Key.create(new byte[]{10}))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey().size()).isOne();
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat(found.getMaxRowKey().size()).isOne();
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType(), new ByteArrayType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(Arrays.asList(new byte[]{1}, new byte[]{2})))
                .maxRowKey(Key.create(Arrays.asList(new byte[]{10}, new byte[]{11})))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType(), new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey().size()).isEqualTo(2);
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat((byte[]) found.getMinRowKey().get(1)).containsExactly(new byte[]{2});
            assertThat(found.getMaxRowKey().size()).isEqualTo(2);
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat((byte[]) found.getMaxRowKey().get(1)).containsExactly(new byte[]{11});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(Arrays.asList(1L, "Z")))
                .maxRowKey(Key.create(Arrays.asList(10L, "A")))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType(), new StringType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(Arrays.asList(1L, "Z")));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(Arrays.asList(10L, "A")));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnAllFileInfos() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10000; i++) { // 10,000 figure chosen to ensure results returned from Dynamo are paged
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("" + i)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo);
            expected.add(fileInfo);
        }

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();

        // Then
        assertThat(fileInfos).hasSize(10000).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingFilename() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingStatus() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingPartition() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testGetFilesThatAreReadyForGC() throws StateStoreException {
        // Given
        Instant file1Time = Instant.parse("2023-06-06T15:00:00Z");
        Instant file2Time = Instant.parse("2023-06-06T15:01:00Z");
        Instant file3Time = Instant.parse("2023-06-06T15:02:00Z");
        Instant file1GCTime = Instant.parse("2023-06-06T15:05:30Z");
        Instant file3GCTime = Instant.parse("2023-06-06T15:07:30Z");
        Schema schema = schemaWithKeyAndValueWithTypes(new IntType(), new StringType());
        DynamoDBStateStore stateStore = getStateStore(schema, 5);
        Partition partition = stateStore.getAllPartitions().get(0);
        //  - A file which should be garbage collected immediately
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(file1Time)
                .build();
        stateStore.addFile(fileInfo1);
        //  - An active file which should not be garbage collected
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(file2Time)
                .build();
        stateStore.addFile(fileInfo2);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(file3Time)
                .build();
        stateStore.addFile(fileInfo3);

        // When / Then 1
        stateStore.fixTime(file1GCTime);
        assertThat(stateStore.getReadyForGCFiles()).toIterable().containsExactly(fileInfo1);

        // When / Then 2
        stateStore.fixTime(file3GCTime);
        assertThat(stateStore.getReadyForGCFiles()).toIterable().containsExactly(fileInfo1, fileInfo3);
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobId() throws StateStoreException {
        // Given
        Schema schema = schemaWithKeyAndValueWithTypes(new LongType(), new StringType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("2")
                .minRowKey(Key.create(20L))
                .maxRowKey(Key.create(29L))
                .lastStateStoreUpdateTime(2_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo2);
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("3")
                .jobId("job1")
                .minRowKey(Key.create(100L))
                .maxRowKey(Key.create(10000L))
                .lastStateStoreUpdateTime(3_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo3);

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFilesWithNoJobId();

        // Then
        assertThat(fileInfos).containsExactly(fileInfo1, fileInfo2);
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobIdWhenPaging() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10000; i++) { // 10,000 figure chosen to ensure results returned from Dyanmo are paged
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("" + i)
                    .minRowKey(Key.create((long) 2 * i))
                    .maxRowKey(Key.create((long) 2 * i + 1))
                    .lastStateStoreUpdateTime((long) i * 1_000)
                    .build();
            dynamoDBStateStore.addFile(fileInfo);
            expected.add(fileInfo);
        }

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFilesWithNoJobId();

        // Then
        assertThat(fileInfos).hasSize(10000).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldDeleteReadyForGCFile() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("4")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .partitionId("5")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(2_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo2);

        // When
        dynamoDBStateStore.deleteReadyForGCFile(fileInfo2);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).containsExactly(fileInfo1);
        assertThat(dynamoDBStateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFile() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        FileInfo newFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .lastStateStoreUpdateTime(10_000_000L)
                .build();

        // When
        dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).containsExactly(newFileInfo);
        assertThat(dynamoDBStateStore.getReadyForGCFiles()).toIterable().hasSize(4);
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesForSplittingJob() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        FileInfo newLeftFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-left-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .lastStateStoreUpdateTime(10_000_000L)
                .build();
        FileInfo newRightFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-right-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .lastStateStoreUpdateTime(10_000_000L)
                .build();

        // When
        dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles()).containsExactlyInAnyOrder(newLeftFileInfo, newRightFileInfo);
        assertThat(dynamoDBStateStore.getReadyForGCFiles()).toIterable().hasSize(4);
    }

    @Test
    public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFileShouldFailIfFilesNotActive() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .build();
            filesToMoveToReadyForGC.add(fileInfo);
        }
        //  - One of the files is not active
        FileInfo updatedFileInfo = filesToMoveToReadyForGC.remove(3).toBuilder()
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .build();
        filesToMoveToReadyForGC.add(3, updatedFileInfo);
        dynamoDBStateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("7")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldAtomicallyUpdateJobStatusOfFiles() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("8")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When
        dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

        // Then
        assertThat(dynamoDBStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId")
                .containsExactlyInAnyOrderElementsOf(files)
                .extracting(FileInfo::getJobId).containsOnly(jobId);
        assertThat(dynamoDBStateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("9")
                    .jobId("compactionJob")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldNotAtomicallyUpdateJobStatusOfFilesIfFileInfoNotPresent() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("8")
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            files.add(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new StringType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("B"))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
        byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithMultidimensionalKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
        byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        byte[] min = new byte[]{1, 2, 3, 4};
        byte[] max = new byte[]{5, 6, 7, 8, 9};
        Range range = new RangeFactory(schema).createRange("key", min, max);
        Partition partition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range))
                .id("id")
                .leafPartition(false)
                .parentPartitionId("P")
                .childPartitionIds(new ArrayList<>())
                .dimension(0)
                .build();
        StateStore dynamoDBStateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = dynamoDBStateStore.getAllPartitions().get(0);

        // Then
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key").getMax());
        assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
        assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
        assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
        assertThat(retrievedPartition.getDimension()).isEqualTo(partition.getDimension());
    }

    // TODO shouldCorrectlyStorePartitionWithMultidimensionalKeyType

    @Test
    public void shouldReturnCorrectPartitionToFileMapping() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("" + (i % 5))
                    .minRowKey(Key.create((long) i % 5))
                    .maxRowKey(Key.create((long) i % 5))
                    .build();
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }

        // When
        Map<String, List<String>> partitionToFileMapping = dynamoDBStateStore.getPartitionToActiveFilesMap();

        // Then
        assertThat(partitionToFileMapping.entrySet()).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(partitionToFileMapping.get("" + i)).hasSize(2);
            Set<String> expected = new HashSet<>();
            expected.add(files.get(i).getFilename());
            expected.add(files.get(i + 5).getFilename());
            assertThat(new HashSet<>(partitionToFileMapping.get("" + i))).isEqualTo(expected);
        }
    }

    @Test
    public void shouldReturnAllPartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        PartitionTree tree = new PartitionsBuilder(schema).rootFirst("root")
                .splitToNewChildren("root", "left", "right", 100L)
                .splitToNewChildren("left", "id1", "id2", 1L)
                .splitToNewChildren("right", "id3", "id4", 200L).buildTree();

        StateStore dynamoDBStateStore = getStateStore(schema, tree.getAllPartitions());

        // When / Then
        assertThat(dynamoDBStateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
    }

    @Test
    public void shouldReturnLeafPartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();

        PartitionTree intermediateTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "id1", "id2", 1L)
                .buildTree();

        PartitionTree expectedTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "id1", "id2", 1L)
                .splitToNewChildren("id2", "id3", "id4", 9L).buildTree();
        DynamoDBStateStore stateStore = getStateStore(schema, tree.getAllPartitions());

        // When
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(intermediateTree.getRootPartition(), intermediateTree.getPartition("id1"), intermediateTree.getPartition("id2"));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(expectedTree.getPartition("id2"), expectedTree.getPartition("id3"), expectedTree.getPartition("id4"));

        // Then
        assertThat(stateStore.getLeafPartitions())
                .containsExactlyInAnyOrderElementsOf(expectedTree.getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList()));
    }

    @Test
    public void shouldUpdatePartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "child1", "child2", 0L)
                .buildTree();

        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getRootPartition(), tree.getPartition("child1"), tree.getPartition("child2"));

        // Then
        assertThat(dynamoDBStateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
    }

    @Test
    public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "child1", "child2", 0L)
                .buildTree();

        dynamoDBStateStore.initialise(tree.getAllPartitions());

        // When / Then
        //  - Attempting to split something that has already been split should fail
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(tree.getPartition("root"), tree.getPartition("child1"), tree.getPartition("child2")))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentIsLeaf() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder().childPartitionIds(Arrays.asList("child1", "child2")).build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child3", "child2")) // Wrong children
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("notparent") // Wrong parent
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        Partition expectedPartition = new PartitionsBuilder(schema).rootFirst(partitions.get(0).getId()).buildTree().getPartition(partitions.get(0).getId());

        // Then
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldReinitialisePartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionTree treeBefore = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "before1", "before2", 0L)
                .buildTree();
        StateStore dynamoDBStateStore = getStateStore(schema, treeBefore.getAllPartitions());

        // When
        PartitionTree treeAfter = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "after1", "after2", 10L)
                .buildTree();

        dynamoDBStateStore.initialise(treeAfter.getAllPartitions());

        // Then
        assertThat(dynamoDBStateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(treeAfter.getAllPartitions());
    }
}
