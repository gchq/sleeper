/*
 * Copyright 2022 Crown Copyright
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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

public class DynamoDBStateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @BeforeClass
    public static void initDynamoClient() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @AfterClass
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private StateStore getStateStore(Schema schema,
                                     List<Partition> partitions,
                                     int garbageCollectorDelayBeforeDeletionInSeconds) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, schema, garbageCollectorDelayBeforeDeletionInSeconds, dynamoDBClient);
        StateStore stateStore = dynamoDBStateStoreCreator.create();
        stateStore.initialise(partitions);
        return stateStore;
    }

    private StateStore getStateStore(Schema schema,
                                     List<Partition> partitions) throws StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints, int garbageCollectorDelayBeforeDeletionInSeconds) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), garbageCollectorDelayBeforeDeletionInSeconds);
    }

    private StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), 0);
    }

    private StateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInSeconds) throws StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.EMPTY_LIST, garbageCollectorDelayBeforeDeletionInSeconds);
    }

    private StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.EMPTY_LIST);
    }

    @Test
    public void shouldReturnCorrectFileInfoForLongRowKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();
        assertEquals(1, fileInfos.size());
        assertEquals(1, fileInfos.get(0).getRowKeyTypes().size());
        assertEquals(new LongType(), fileInfos.get(0).getRowKeyTypes().get(0));
        assertEquals("abc", fileInfos.get(0).getFilename());
        assertEquals(FileInfo.FileStatus.ACTIVE, fileInfos.get(0).getFileStatus());
        assertEquals("1", fileInfos.get(0).getPartitionId());
        assertEquals(Key.create(1L), fileInfos.get(0).getMinRowKey());
        assertEquals(Key.create(10L), fileInfos.get(0).getMaxRowKey());
        assertEquals(1_000_000L, fileInfos.get(0).getLastStateStoreUpdateTime().longValue());
    }

    @Test
    public void shouldReturnCorrectFileInfoForByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new ByteArrayType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(new byte[]{1}));
        fileInfo.setMaxRowKey(Key.create(new byte[]{10}));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();
        assertEquals(1, fileInfos.size());
        assertEquals(1, fileInfos.get(0).getRowKeyTypes().size());
        assertEquals(new ByteArrayType(), fileInfos.get(0).getRowKeyTypes().get(0));
        assertEquals("abc", fileInfos.get(0).getFilename());
        assertEquals(FileInfo.FileStatus.ACTIVE, fileInfos.get(0).getFileStatus());
        assertEquals("1", fileInfos.get(0).getPartitionId());
        assertEquals(1, fileInfos.get(0).getMinRowKey().size());
        assertArrayEquals(new byte[]{1}, (byte[]) fileInfos.get(0).getMinRowKey().get(0));
        assertEquals(1, fileInfos.get(0).getMaxRowKey().size());
        assertArrayEquals(new byte[]{10}, (byte[]) fileInfos.get(0).getMaxRowKey().get(0));
        assertEquals(1_000_000L, fileInfos.get(0).getLastStateStoreUpdateTime().longValue());
    }

    @Test
    public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(Arrays.asList(new byte[]{1}, new byte[]{2})));
        fileInfo.setMaxRowKey(Key.create(Arrays.asList(new byte[]{10}, new byte[]{11})));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();
        assertEquals(1, fileInfos.size());
        assertEquals(2, fileInfos.get(0).getRowKeyTypes().size());
        assertEquals(new ByteArrayType(), fileInfos.get(0).getRowKeyTypes().get(0));
        assertEquals(new ByteArrayType(), fileInfos.get(0).getRowKeyTypes().get(1));
        assertEquals("abc", fileInfos.get(0).getFilename());
        assertEquals(FileInfo.FileStatus.ACTIVE, fileInfos.get(0).getFileStatus());
        assertEquals("1", fileInfos.get(0).getPartitionId());
        assertEquals(2, fileInfos.get(0).getMinRowKey().size());
        assertArrayEquals(new byte[]{1}, (byte[]) fileInfos.get(0).getMinRowKey().get(0));
        assertArrayEquals(new byte[]{2}, (byte[]) fileInfos.get(0).getMinRowKey().get(1));
        assertEquals(2, fileInfos.get(0).getMaxRowKey().size());
        assertArrayEquals(new byte[]{10}, (byte[]) fileInfos.get(0).getMaxRowKey().get(0));
        assertArrayEquals(new byte[]{11}, (byte[]) fileInfos.get(0).getMaxRowKey().get(1));
        assertEquals(1_000_000L, fileInfos.get(0).getLastStateStoreUpdateTime().longValue());
    }

    @Test
    public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new LongType()), new Field("key2", new StringType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType(), new StringType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(Arrays.asList(1L, "Z")));
        fileInfo.setMaxRowKey(Key.create(Arrays.asList(10L, "A")));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When
        dynamoDBStateStore.addFile(fileInfo);

        // Then
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();
        assertEquals(1, fileInfos.size());
        assertEquals("abc", fileInfo.getFilename());
        assertEquals(2, fileInfos.get(0).getRowKeyTypes().size());
        assertEquals(new LongType(), fileInfos.get(0).getRowKeyTypes().get(0));
        assertEquals(new StringType(), fileInfos.get(0).getRowKeyTypes().get(1));
        assertEquals("abc", fileInfos.get(0).getFilename());
        assertEquals(FileInfo.FileStatus.ACTIVE, fileInfos.get(0).getFileStatus());
        assertEquals("1", fileInfos.get(0).getPartitionId());
        assertEquals(Key.create(Arrays.asList(1L, "Z")), fileInfos.get(0).getMinRowKey());
        assertEquals(Key.create(Arrays.asList(10L, "A")), fileInfos.get(0).getMaxRowKey());
        assertEquals(1_000_000L, fileInfos.get(0).getLastStateStoreUpdateTime().longValue());
    }

    @Test
    public void shouldReturnAllFileInfos() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10000; i++) { // 10,000 figure chosen to ensure results returned from Dynamo are paged
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("" + i);
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(1_000_000L);
            dynamoDBStateStore.addFile(fileInfo);
            expected.add(fileInfo);
        }

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFiles();

        // Then
        assertEquals(10000, fileInfos.size());
        assertEquals(expected, new HashSet<>(fileInfos));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWhenAddingFileInfoWithMissingFilename() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When / Then
        dynamoDBStateStore.addFile(fileInfo);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWhenAddingFileInfoWithMissingStatus() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFilename("abc");
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When / Then
        dynamoDBStateStore.addFile(fileInfo);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionThrownWhenAddingFileInfoWithMissingPartition() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);

        // When / Then
        dynamoDBStateStore.addFile(fileInfo);
    }

    @Test
    public void testGetFilesThatAreReadyForGC() throws InterruptedException, StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new IntType()));
        schema.setValueFields(new Field("value", new StringType()));
        StateStore stateStore = getStateStore(schema, 5);
        Partition partition = stateStore.getAllPartitions().get(0);
        //  - A file which should be garbage collected immediately
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo1.setPartitionId(partition.getId());
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(100));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setLastStateStoreUpdateTime(System.currentTimeMillis() - 8000);
        stateStore.addFile(fileInfo1);
        //  - An active file which should not be garbage collected
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(partition.getId());
        fileInfo2.setMinRowKey(Key.create(1));
        fileInfo2.setMaxRowKey(Key.create(100));
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setLastStateStoreUpdateTime(System.currentTimeMillis());
        stateStore.addFile(fileInfo2);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        FileInfo fileInfo3 = new FileInfo();
        fileInfo3.setRowKeyTypes(new IntType());
        fileInfo3.setFilename("file3");
        fileInfo3.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo3.setPartitionId(partition.getId());
        fileInfo3.setMinRowKey(Key.create(1));
        fileInfo3.setMaxRowKey(Key.create(100));
        fileInfo3.setNumberOfRecords(100L);
        fileInfo3.setLastStateStoreUpdateTime(System.currentTimeMillis());
        stateStore.addFile(fileInfo3);

        // When 1
        Iterator<FileInfo> readyForGCFilesIterator = stateStore.getReadyForGCFiles();
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        while (readyForGCFilesIterator.hasNext()) {
            readyForGCFiles.add(readyForGCFilesIterator.next());
        }

        // Then 1
        assertEquals(1, readyForGCFiles.size());
        assertEquals(fileInfo1, readyForGCFiles.get(0));

        // When 2
        Thread.sleep(9000L);
        readyForGCFilesIterator = stateStore.getReadyForGCFiles();
        readyForGCFiles.clear();
        while (readyForGCFilesIterator.hasNext()) {
            readyForGCFiles.add(readyForGCFilesIterator.next());
        }

        // Then 2
        assertEquals(2, readyForGCFiles.size());
        if (readyForGCFiles.get(0).getFilename().equals(fileInfo1.getFilename())) {
            assertEquals(fileInfo1, readyForGCFiles.get(0));
            assertEquals(fileInfo3, readyForGCFiles.get(1));
        } else {
            assertEquals(fileInfo3, readyForGCFiles.get(0));
            assertEquals(fileInfo1, readyForGCFiles.get(1));
        }
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobId() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("value", new StringType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setMinRowKey(Key.create(1L));
        fileInfo1.setMaxRowKey(Key.create(10L));
        fileInfo1.setLastStateStoreUpdateTime(1_000_000L);
        dynamoDBStateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("2");
        fileInfo2.setMinRowKey(Key.create(20L));
        fileInfo2.setMaxRowKey(Key.create(29L));
        fileInfo2.setLastStateStoreUpdateTime(2_000_000L);
        dynamoDBStateStore.addFile(fileInfo2);
        FileInfo fileInfo3 = new FileInfo();
        fileInfo3.setRowKeyTypes(new LongType());
        fileInfo3.setFilename("file3");
        fileInfo3.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo3.setPartitionId("3");
        fileInfo3.setJobId("job1");
        fileInfo3.setMinRowKey(Key.create(100L));
        fileInfo3.setMaxRowKey(Key.create(10000L));
        fileInfo3.setLastStateStoreUpdateTime(3_000_000L);
        dynamoDBStateStore.addFile(fileInfo3);

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFilesWithNoJobId();

        // Then
        assertEquals(2, fileInfos.size());
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(fileInfo1);
        expectedFileInfos.add(fileInfo2);
        assertEquals(expectedFileInfos, fileInfos);
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobIdWhenPaging() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10000; i++) { // 10,000 figure chosen to ensure results returned from Dyanmo are paged
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("" + i);
            fileInfo.setMinRowKey(Key.create((long) 2 * i));
            fileInfo.setMaxRowKey(Key.create((long) 2 * i + 1));
            fileInfo.setLastStateStoreUpdateTime((long) i * 1_000);
            dynamoDBStateStore.addFile(fileInfo);
            expected.add(fileInfo);
        }

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getActiveFilesWithNoJobId();

        // Then
        assertEquals(10000, fileInfos.size());
        assertEquals(expected, new HashSet<>(fileInfos));
    }

    @Test
    public void shouldDeleteReadyForGCFile() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("4");
        fileInfo1.setMinRowKey(Key.create(1L));
        fileInfo1.setMaxRowKey(Key.create(10L));
        fileInfo1.setLastStateStoreUpdateTime(1_000_000L);
        dynamoDBStateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo2.setPartitionId("5");
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(10L));
        fileInfo2.setLastStateStoreUpdateTime(2_000_000L);
        dynamoDBStateStore.addFile(fileInfo2);

        // When
        dynamoDBStateStore.deleteReadyForGCFile(fileInfo2);
        List<FileInfo> active = dynamoDBStateStore.getActiveFiles();
        Iterator<FileInfo> readyForGC = dynamoDBStateStore.getReadyForGCFiles();

        // Then
        assertEquals(1, active.size());
        assertFalse(readyForGC.hasNext());
        assertEquals(fileInfo1, active.get(0));
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFile() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("7");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(i * 1_000_000L);
            filesToMoveToReadyForGC.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        FileInfo newFileInfo = new FileInfo();
        newFileInfo.setRowKeyTypes(new LongType());
        newFileInfo.setFilename("file-new");
        newFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFileInfo.setPartitionId("7");
        newFileInfo.setLastStateStoreUpdateTime(10_000_000L);

        // When
        dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo);
        List<FileInfo> filesReadyForGC = new ArrayList<>();
        Iterator<FileInfo> filesReadyForGCIterator = dynamoDBStateStore.getReadyForGCFiles();
        while (filesReadyForGCIterator.hasNext()) {
            filesReadyForGC.add(filesReadyForGCIterator.next());
        }
        List<FileInfo> activeFiles = dynamoDBStateStore.getActiveFiles();

        // Then
        assertEquals(4, filesReadyForGC.size());
        assertEquals(1, activeFiles.size());
        assertEquals(newFileInfo, activeFiles.get(0));
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesForSplittingJob() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("7");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(i * 1_000_000L);
            filesToMoveToReadyForGC.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        FileInfo newLeftFileInfo = new FileInfo();
        newLeftFileInfo.setRowKeyTypes(new LongType());
        newLeftFileInfo.setFilename("file-left-new");
        newLeftFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newLeftFileInfo.setPartitionId("7");
        newLeftFileInfo.setLastStateStoreUpdateTime(10_000_000L);
        FileInfo newRightFileInfo = new FileInfo();
        newRightFileInfo.setRowKeyTypes(new LongType());
        newRightFileInfo.setFilename("file-right-new");
        newRightFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newRightFileInfo.setPartitionId("7");
        newRightFileInfo.setLastStateStoreUpdateTime(10_000_000L);

        // When
        dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo);
        List<FileInfo> filesReadyForGC = new ArrayList<>();
        Iterator<FileInfo> filesReadyForGCIterator = dynamoDBStateStore.getReadyForGCFiles();
        while (filesReadyForGCIterator.hasNext()) {
            filesReadyForGC.add(filesReadyForGCIterator.next());
        }
        List<FileInfo> activeFiles = dynamoDBStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename)) // Put the left file first to make the following tests simpler
                .collect(Collectors.toList());

        // Then
        assertEquals(4, filesReadyForGC.size());
        assertEquals(2, activeFiles.size());
        assertEquals(newLeftFileInfo, activeFiles.get(0));
        assertEquals(newRightFileInfo, activeFiles.get(1));
    }

    @Test(expected = StateStoreException.class)
    public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFileShouldFailIfFilesNotActive() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("7");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            filesToMoveToReadyForGC.add(fileInfo);
        }
        //  - One of the files is not active
        filesToMoveToReadyForGC.get(3).setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        dynamoDBStateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newFileInfo = new FileInfo();
        newFileInfo.setRowKeyTypes(new LongType());
        newFileInfo.setFilename("file-new");
        newFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFileInfo.setPartitionId("7");

        // When / Then
        dynamoDBStateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo);
    }

    @Test
    public void shouldAtomicallyUpdateJobStatusOfFiles() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("8");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(i * 1_000_000L);
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When
        dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

        // Then
        Set<FileInfo> updatedFiles = new HashSet<>(dynamoDBStateStore.getActiveFiles());
        assertEquals(4, updatedFiles.size());
        Set<FileInfo> expectedFiles = files.stream()
                .map(f -> {
                    f.setJobId(jobId);
                    return f;
                })
                .collect(Collectors.toSet());
        assertEquals(expectedFiles, updatedFiles);
        assertFalse(dynamoDBStateStore.getReadyForGCFiles().hasNext());
    }

    @Test(expected = StateStoreException.class)
    public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("9");
            fileInfo.setJobId("compactionJob");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(i * 1_000_000L);
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When / Then
        dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When
        List<Partition> allPartitions = stateStore.getAllPartitions();

        // Then
        assertEquals(new HashSet<>(partitions), new HashSet<>(allPartitions));
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new StringType()));
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("B"))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When
        List<Partition> allPartitions = stateStore.getAllPartitions();

        // Then
        assertEquals(new HashSet<>(partitions), new HashSet<>(allPartitions));
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
        byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When
        List<Partition> allPartitions = stateStore.getAllPartitions();

        // Then
        assertEquals(new HashSet<>(partitions), new HashSet<>(allPartitions));
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithMultidimensionalKeyType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()));
        byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
        byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When
        List<Partition> allPartitions = stateStore.getAllPartitions();

        // Then
        assertEquals(new HashSet<>(partitions), new HashSet<>(allPartitions));
    }

    @Test
    public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        byte[] min = new byte[]{1, 2, 3, 4};
        byte[] max = new byte[]{5, 6, 7, 8, 9};
        Range range = new RangeFactory(schema).createRange(field.getName(), min, max);
        Partition partition = new Partition(schema.getRowKeyTypes(),
                new Region(range), "id", false, "P", new ArrayList<>(), 0);
        StateStore dynamoDBStateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = dynamoDBStateStore.getAllPartitions().get(0);

        // Then
        Assert.assertArrayEquals((byte[]) partition.getRegion().getRange("key").getMin(), (byte[]) retrievedPartition.getRegion().getRange("key").getMin());
        Assert.assertArrayEquals((byte[]) partition.getRegion().getRange("key").getMax(), (byte[]) retrievedPartition.getRegion().getRange("key").getMax());
        assertEquals(partition.getId(), retrievedPartition.getId());
        assertEquals(partition.getParentPartitionId(), retrievedPartition.getParentPartitionId());
        assertEquals(partition.getChildPartitionIds(), retrievedPartition.getChildPartitionIds());
        assertEquals(partition.getDimension(), retrievedPartition.getDimension());
    }

    // TODO shouldCorrectlyStorePartitionWithMultidimensionalKeyType

    @Test
    public void shouldReturnCorrectPartitionToFileMapping() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("" + (i % 5));
            fileInfo.setMinRowKey(Key.create((long) i % 5));
            fileInfo.setMaxRowKey(Key.create((long) i % 5));
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }

        // When
        Map<String, List<String>> partitionToFileMapping = dynamoDBStateStore.getPartitionToActiveFilesMap();

        // Then
        assertEquals(5, partitionToFileMapping.entrySet().size());
        for (int i = 0; i < 5; i++) {
            assertEquals(2, partitionToFileMapping.get("" + i).size());
            Set<String> expected = new HashSet<>();
            expected.add(files.get(i).getFilename());
            expected.add(files.get(i + 5).getFilename());
            assertEquals(expected, new HashSet<>(partitionToFileMapping.get("" + i)));
        }
    }

    @Test
    public void shouldReturnAllPartitions() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        Region region0 = new Region(new Range(field, Long.MIN_VALUE, 1L));
        Partition partition0 = new Partition(schema.getRowKeyTypes(), region0, "id0", true, "root", new ArrayList<>(), -1);
        Region region1 = new Region(new Range(field, 1L, 100L));
        Partition partition1 = new Partition(schema.getRowKeyTypes(), region1, "id1", true, "root", new ArrayList<>(), -1);
        Region region2 = new Region(new Range(field, 100L, 200L));
        Partition partition2 = new Partition(schema.getRowKeyTypes(), region2, "id2", true, "root", new ArrayList<>(), -1);
        Region region3 = new Region(new Range(field, 200L, null));
        Partition partition3 = new Partition(schema.getRowKeyTypes(), region3, "id3", true, "root", new ArrayList<>(), -1);
        StateStore dynamoDBStateStore = getStateStore(schema, Arrays.asList(partition0, partition1, partition2, partition3));

        // When
        List<Partition> retrievedPartitions = dynamoDBStateStore.getAllPartitions();
        retrievedPartitions.sort((p1, p2) -> {
            long p1Key = (long) p1.getRegion().getRange("key").getMin();
            long p2Key = (long) p2.getRegion().getRange("key").getMin();
            if (p1Key < p2Key) {
                return -1;
            } else if (p1Key == p2Key) {
                return 0;
            }
            return 1;
        });

        // Then
        assertEquals(partition0, retrievedPartitions.get(0));
        assertEquals(partition1, retrievedPartitions.get(1));
        assertEquals(partition2, retrievedPartitions.get(2));
        assertEquals(partition3, retrievedPartitions.get(3));
    }

    @Test
    public void shouldReturnLeafPartitions() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition rootPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, 1L));
        Partition partition1 = new Partition(schema.getRowKeyTypes(), region1, "id1", true, rootPartition.getId(), new ArrayList<>(), -1);
        Region region2 = new Region(new Range(field, 1L, null));
        Partition partition2 = new Partition(schema.getRowKeyTypes(), region2, "id2", true, rootPartition.getId(), new ArrayList<>(), -1);
        rootPartition.setLeafPartition(false);
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, partition1, partition2);
        Region region3 = new Region(new Range(field, 1L, 9L));
        Partition partition3 = new Partition(schema.getRowKeyTypes(), region3, "id3", true, partition2.getId(), new ArrayList<>(), -1);
        Region region4 = new Region(new Range(field, 9L, null));
        Partition partition4 = new Partition(schema.getRowKeyTypes(), region4, "id4", true, partition2.getId(), new ArrayList<>(), -1);
        partition2.setLeafPartition(false);
        partition2.setChildPartitionIds(Arrays.asList(partition3.getId(), partition4.getId()));
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(partition2, partition3, partition4);

        // When
        List<Partition> retrievedPartitions = dynamoDBStateStore.getLeafPartitions();
        retrievedPartitions.sort((p1, p2) -> {
            long p1Key = (long) p1.getRegion().getRange("key").getMin();
            long p2Key = (long) p2.getRegion().getRange("key").getMin();
            if (p1Key < p2Key) {
                return -1;
            } else if (p1Key == p2Key) {
                return 0;
            }
            return 1;
        });

        // Then
        assertEquals(partition1, retrievedPartitions.get(0));
        assertEquals(partition3, retrievedPartitions.get(1));
        assertEquals(partition4, retrievedPartitions.get(2));
    }

    @Test
    public void shouldUpdatePartitions() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);

        // When
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        parentPartition.setDimension(1);
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, 0L));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId(parentPartition.getId());
        childPartition1.setDimension(-1);
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new Range(field, 0L, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId(parentPartition.getId());
        childPartition2.setDimension(-1);
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertEquals(new HashSet<>(Arrays.asList(parentPartition, childPartition1, childPartition2)), new HashSet<>(partitions));
    }

    @Test(expected = StateStoreException.class)
    public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = null;
        try {
            dynamoDBStateStore = getStateStore(schema);
        } catch (StateStoreException e) {
            fail(e.getMessage());
        }
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, 0L));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId(parentPartition.getId());
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new Range(field, 0L, null));
        childPartition2.setRegion(region2);
        
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId(parentPartition.getId());
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);

        // When / Then
        //  - Attempting to split something that has already been split should fail
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);
    }

    @Test(expected = StateStoreException.class)
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentIsLeaf() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = null;
        try {
            dynamoDBStateStore = getStateStore(schema);
        } catch (StateStoreException e) {
            fail(e.getMessage());
        }
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, null));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("parent");
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new Range(field, Long.MIN_VALUE, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

        // When / Then
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);
    }

    @Test(expected = StateStoreException.class)
    public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = null;
        try {
            dynamoDBStateStore = getStateStore(schema);
        } catch (StateStoreException e) {
            fail(e.getMessage());
        }
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child3", "child2")); // Wrong children
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, null));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("parent");
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new Range(field, Long.MIN_VALUE, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

        // When / Then
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);
    }

    @Test(expected = StateStoreException.class)
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = null;
        try {
            dynamoDBStateStore = getStateStore(schema);
        } catch (StateStoreException e) {
            fail(e.getMessage());
        }
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, null));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("notparent"); // Wrong parent
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new Range(field, Long.MIN_VALUE, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

        // When / Then
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);
    }

    @Test(expected = StateStoreException.class)
    public void shouldThrowExceptionWithPartitionSplitRequestWhereNewPartitionIsNotLeaf() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = null;
        try {
            dynamoDBStateStore = getStateStore(schema);
        } catch (StateStoreException e) {
            fail(e.getMessage());
        }
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new Range(field, Long.MIN_VALUE, 0L));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("parent");
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(false); // Not leaf
        childPartition2.setId("child2");
        Region region2 = new Region(new Range(field, 0L, Long.MAX_VALUE));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

        // When / Then
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertEquals(1, partitions.size());
        Region expectedRegion = new Region(new Range(field, Integer.MIN_VALUE, null));
        Partition expectedPartition = new Partition(schema.getRowKeyTypes(),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
        assertEquals(expectedPartition, partitions.get(0));
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertEquals(1, partitions.size());
        Region expectedRegion = new Region(new Range(field, Long.MIN_VALUE, null));
        Partition expectedPartition = new Partition(Collections.singletonList(new LongType()),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
        assertEquals(expectedPartition, partitions.get(0));
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new StringType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertEquals(1, partitions.size());
        Region expectedRegion = new Region(new Range(field, "", null));
        Partition expectedPartition = new Partition(Collections.singletonList(new StringType()),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
        assertEquals(expectedPartition, partitions.get(0));
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertEquals(1, partitions.size());
        Region expectedRegion = new Region(new Range(field, new byte[]{}, null));
        Partition expectedPartition = new Partition(Collections.singletonList(new ByteArrayType()),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
        assertEquals(expectedPartition, partitions.get(0));
    }
}
