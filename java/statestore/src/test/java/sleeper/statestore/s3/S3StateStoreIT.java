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
package sleeper.statestore.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoFactory;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
public class S3StateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @TempDir
    public Path folder;

    @BeforeAll
    public static void initDynamoClient() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private String createDynamoTable() {
        String tableName = UUID.randomUUID().toString();
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(S3StateStore.REVISION_ID_KEY, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(S3StateStore.REVISION_ID_KEY, KeyType.HASH));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        dynamoDBClient.createTable(request);
        return tableName;
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions,
                                       int garbageCollectorDelayBeforeDeletionInMinutes) throws IOException, StateStoreException {
        String bucket = createTempDirectory(folder, null).toString();
        String dynamoTableName = createDynamoTable();
        S3StateStore stateStore = new S3StateStore("", 5, bucket, dynamoTableName, schema, garbageCollectorDelayBeforeDeletionInMinutes, dynamoDBClient, new Configuration());
        stateStore.initialise(partitions);
        return stateStore;
    }

    private S3StateStore getStateStore(Schema schema,
                                       List<Partition> partitions) throws IOException, StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private S3StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints) throws IOException, StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), 0);
    }

    private S3StateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) throws IOException, StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct(), garbageCollectorDelayBeforeDeletionInMinutes);
    }

    private S3StateStore getStateStore(Schema schema) throws IOException, StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.EMPTY_LIST);
    }

    private Schema schemaWithSingleRowKeyType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private Schema schemaWithTwoRowKeyTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field("key1", type1), new Field("key2", type2)).build();
    }

    @Test
    public void shouldReturnCorrectFileInfoForLongRowKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(1L));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(10L));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoForByteArrayKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(new byte[]{1}))
                .maxRowKey(Key.create(new byte[]{10}))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey().size()).isOne();
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat(found.getMaxRowKey().size()).isOne();
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType(), new ByteArrayType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(Arrays.asList(new byte[]{1}, new byte[]{2})))
                .maxRowKey(Key.create(Arrays.asList(new byte[]{10}, new byte[]{11})))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType(), new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.FILE_IN_PARTITION);
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
    public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(Arrays.asList(1L, "Z")))
                .maxRowKey(Key.create(Arrays.asList(10L, "A")))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType(), new StringType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileInfo.FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("1");
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(Arrays.asList(1L, "Z")));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(Arrays.asList(10L, "A")));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
    }

    @Test
    public void shouldReturnAllFileInfos() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("" + i)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .numberOfRecords(1L)
                    .build();
            expected.add(fileInfo);
        }
        stateStore.addFiles(new ArrayList<>(expected));

        // When
        List<FileInfo> fileInfos = stateStore.getFileInPartitionList();

        // Then
        assertThat(fileInfos).hasSize(10000).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingFilename() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();

        // When / Then
        assertThatThrownBy(() -> stateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // @Test
    // public void testExceptionThrownWhenAddingFileInfoWithMissingStatus() throws IOException, StateStoreException {
    //     // Given
    //     Schema schema = schemaWithSingleRowKeyType(new LongType());
    //     StateStore stateStore = getStateStore(schema);
    //     FileInfo fileInfo = FileInfo.builder()
    //             .rowKeyTypes(new LongType())
    //             .filename("abc")
    //             .partitionId("1")
    //             .minRowKey(Key.create(1L))
    //             .maxRowKey(Key.create(10L))
    //             .lastStateStoreUpdateTime(1_000_000L)
    //             .numberOfRecords(1L)
    //             .build();

    //     // When / Then
    //     assertThatThrownBy(() -> stateStore.addFile(fileInfo))
    //             .isInstanceOf(IllegalArgumentException.class);
    // }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingPartition() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();

        // When / Then
        assertThatThrownBy(() -> stateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldAddFilesUnderContention() throws IOException, StateStoreException, InterruptedException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("root")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }

        // When
        for (int i = 0; i < 20; i++) {
            final FileInfo fileInfo = files.get(i);
            executorService.execute(() -> {
                try {
                    stateStore.addFile(fileInfo);
                } catch (StateStoreException e) {
                    e.printStackTrace();
                }
            });
        }
        Thread.sleep(5000L);

        // Then
        List<FileInfo> fileInPartitionList = stateStore.getFileInPartitionList();
        int retries = 0;
        while (fileInPartitionList.size() < 20 && retries < 5) {
            Thread.sleep(3000L);
            retries++;
            fileInPartitionList = stateStore.getFileInPartitionList();
        }
        assertThat(fileInPartitionList).hasSize(20).containsExactlyInAnyOrderElementsOf(files);
        executorService.shutdown();
    }

    @Test
    public void testGetFilesThatAreReadyForGC() throws IOException, InterruptedException, StateStoreException {
        // Given
        Instant file1Time = Instant.parse("2023-06-06T15:00:00Z");
        Instant file2Time = Instant.parse("2023-06-06T15:01:00Z");
        Instant file3Time = Instant.parse("2023-06-06T15:02:00Z");
        Instant file1GCTime = Instant.parse("2023-06-06T15:05:30Z");
        Instant file3GCTime = Instant.parse("2023-06-06T15:07:30Z");
        Schema schema = schemaWithKeyAndValueWithTypes(new IntType(), new StringType());
        S3StateStore stateStore = getStateStore(schema, 5);
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
        FileInfo file3 = factory2.rootFile("file3", 100L, "a", "b");
        dynamoDBStateStore.addFile(file3);
        FileInfo file4 = factory2.rootFile("file4", 100L, "a", "b");
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file3.cloneWithStatus(FileInfo.FileStatus.FILE_IN_PARTITION)),
                file4);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        FileInfo file5 = factory2.rootFile("file5", 100L, "a", "b");
        dynamoDBStateStore.addFile(file5);

        // When / Then 1
        Thread.sleep(5000L);
        List<String> readyForGCFiles = new ArrayList<>();
        dynamoDBStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        assertThat(readyForGCFiles).hasSize(1);
        assertThat(readyForGCFiles.get(0)).isEqualTo("file1");

        // When / Then 2
        dynamoDBStateStore.setStatusToReadyForGarbageCollection(file3.getFilename());
        Thread.sleep(5000L);
        readyForGCFiles.clear();
        dynamoDBStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        assertThat(readyForGCFiles).hasSize(2);
        assertThat(readyForGCFiles.stream().collect(Collectors.toSet())).containsExactlyInAnyOrder("file1", "file3");
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobId() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("1")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();
        stateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("2")
                .minRowKey(Key.create(20L))
                .maxRowKey(Key.create(29L))
                .lastStateStoreUpdateTime(2_000_000L)
                .build();
        stateStore.addFile(fileInfo2);
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("3")
                .jobId("job1")
                .minRowKey(Key.create(100L))
                .maxRowKey(Key.create(10000L))
                .lastStateStoreUpdateTime(3_000_000L)
                .numberOfRecords(3L)
                .build();
        stateStore.addFile(fileInfo3);

        // When
        List<FileInfo> fileInfos = stateStore.getFileInPartitionInfosWithNoJobId();

        // Then
        assertThat(fileInfos).containsExactly(fileInfo1, fileInfo2);
    }

    @Test
    public void shouldDeleteReadyForGCFile() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file1")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("4")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file2")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("5")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(2_000_000L)
                .numberOfRecords(2L)
                .build();
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file3")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("5")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(2_000_000L)
                .numberOfRecords(2L)
                .build();
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        // - Use the following method to remove the file in partition record for fileInfo2
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Arrays.asList(fileInfo2), fileInfo3);
        stateStore.setStatusToReadyForGarbageCollection(fileInfo2.getFilename());

        // When
        stateStore.deleteFileLifecycleEntries(Collections.singletonList(fileInfo2.getFilename()));

        // Then
        assertThat(stateStore.getFileInPartitionList()).containsExactlyInAnyOrder(fileInfo1, fileInfo3);
        assertThat(stateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldatomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> fileInPartitionRecordsToRemove = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .numberOfRecords(1L)
                    .build();
            fileInPartitionRecordsToRemove.add(fileInfo);
            stateStore.addFile(fileInfo);
        }
        FileInfo newFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(10_000_000L)
                .numberOfRecords(4L)
                .build();

        // When
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(fileInPartitionRecordsToRemove, newFileInfo);

        // Then
        assertThat(stateStore.getFileInPartitionList()).containsExactly(newFileInfo);
        assertThat(stateStore.getFileLifecycleList()).hasSize(5);
    }

    @Test
    public void shouldatomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFileForSplittingJob() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> fileInPartitionRecordsToRemove = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .numberOfRecords((long) i)
                    .build();
            fileInPartitionRecordsToRemove.add(fileInfo);
        }
        stateStore.addFiles(fileInPartitionRecordsToRemove);
        FileInfo newLeftFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-left-new")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(5L))
                .lastStateStoreUpdateTime(10_000_000L)
                .numberOfRecords(5L)
                .build();
        FileInfo newRightFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-right-new")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(5L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(10_000_000L)
                .numberOfRecords(5L)
                .build();

        // When
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(fileInPartitionRecordsToRemove, newLeftFileInfo, newRightFileInfo);

        // Then
        assertThat(stateStore.getFileInPartitionList()).containsExactlyInAnyOrder(newLeftFileInfo, newRightFileInfo);
        assertThat(stateStore.getFileLifecycleList()).hasSize(6);
    }

    @Test
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFileShouldFailIfNoFileInPartitionRecord() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> fileInPartitionRecordsToRemove = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .numberOfRecords(1L)
                    .build();
            fileInPartitionRecordsToRemove.add(fileInfo);
        }
        stateStore.addFiles(fileInPartitionRecordsToRemove);
        FileInfo newFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();
        FileInfo newFileInfo2FileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new2")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .numberOfRecords(1L)
                .build();
        //  - Remove one of the file in partition records
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
                Collections.singletonList(fileInPartitionRecordsToRemove.get(3)), newFileInfo2FileInfo);

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(fileInPartitionRecordsToRemove, newFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFileaShouldFailIfNoFileInPartitionRecord() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> fileInPartitionRecordsToRemove = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("7")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .numberOfRecords((long) i)
                    .build();
            fileInPartitionRecordsToRemove.add(fileInfo);
        }
        stateStore.addFiles(fileInPartitionRecordsToRemove);
        FileInfo newLeftFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-left-new")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(5L))
                .lastStateStoreUpdateTime(10_000_000L)
                .numberOfRecords(5L)
                .build();
        FileInfo newRightFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-right-new")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(5L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(10_000_000L)
                .numberOfRecords(5L)
                .build();
        FileInfo dummyFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-dummy")
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .minRowKey(Key.create(5L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(10_000_000L)
                .numberOfRecords(5L)
                .build();
        //  - Remove one of the file in partition records
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
                Collections.singletonList(fileInPartitionRecordsToRemove.get(3)), dummyFileInfo);

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(fileInPartitionRecordsToRemove, newLeftFileInfo, newRightFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldAtomicallyUpdateJobStatusOfFiles() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("8")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);
        String jobId = UUID.randomUUID().toString();

        // When
        stateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

        // Then
        assertThat(stateStore.getFileInPartitionList()).hasSize(4)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId")
                .containsExactlyInAnyOrderElementsOf(files)
                .extracting(FileInfo::getJobId).containsOnly(jobId);
        assertThat(stateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldReturnCorrectFileInPartitionList() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                    .partitionId("8")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);

        // When
        List<FileInfo> fileInPartitionList = stateStore.getFileInPartitionList();

        // Then
        assertThat(fileInPartitionList).hasSize(4)
                .containsExactlyInAnyOrderElementsOf(files);
    }

    @Test
    public void shouldReturnCorrectFileLifecycleList() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);

        // When
        List<FileInfo> fileLifecycleList = stateStore.getFileLifecycleList();

        // Then
        assertThat(fileLifecycleList).hasSize(4)
                .containsExactlyInAnyOrderElementsOf(files);
    }

    @Test
    public void shouldReturnCorrectActiveFileList() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files.subList(0, 3));
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(files.get(2)), files.get(3));
        stateStore.setStatusToReadyForGarbageCollection(files.get(2).getFilename());

        // When
        List<FileInfo> activeFileList = stateStore.getActiveFileList();

        // Then
        List<FileInfo> expected = Arrays.asList(files.get(0), files.get(1), files.get(3));
        assertThat(activeFileList).hasSize(3).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
                    .numberOfRecords(1L)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() -> stateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new StringType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("A"))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        byte[] min = new byte[]{1, 2, 3, 4};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(min))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyStorePartitionWithMultidimensionalKeyType() throws IOException, StateStoreException {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        byte[] min1 = new byte[]{1, 2, 3, 4};
        byte[] min2 = new byte[]{99, 5};
        byte[] max1 = new byte[]{5, 6, 7, 8, 9};
        byte[] max2 = new byte[]{101, 0};
        Range range1 = rangeFactory.createRange(field1, min1, max1);
        Range range2 = rangeFactory.createRange(field2, min2, max2);
        Region region = new Region(Arrays.asList(range1, range2));
        Partition partition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region)
                .id("id")
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        StateStore stateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = stateStore.getAllPartitions().get(0);

        // Then
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key1").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key1").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key1").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key1").getMax());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key2").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key2").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key2").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key2").getMax());
        assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
        assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
        assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
    }

    @Test
    public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        byte[] min = new byte[]{1, 2, 3, 4};
        byte[] max = new byte[]{5, 6, 7, 8, 9};
        Range range = new RangeFactory(schema).createRange(field.getName(), min, max);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region)
                .id("id")
                .leafPartition(false)
                .parentPartitionId("P")
                .childPartitionIds(new ArrayList<>())
                .dimension(0)
                .build();
        StateStore stateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = stateStore.getAllPartitions().get(0);

        // Then
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key").getMax());
        assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
        assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
        assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
        assertThat(retrievedPartition.getDimension()).isEqualTo(partition.getDimension());
    }

    @Test
    public void shouldReturnCorrectPartitionToFileMapping() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .partitionId("" + (i % 5))
                    .minRowKey(Key.create((long) i % 5))
                    .maxRowKey(Key.create((long) i % 5))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .numberOfRecords((long) i)
                    .build();
            files.add(fileInfo);
        }
        stateStore.addFiles(files);

        // When
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToFileInPartitionMap();

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
    public void shouldReturnAllPartitions() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Region region0 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 1L));
        Partition partition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region0)
                .id("id0")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region1 = new Region(new RangeFactory(schema).createRange(field, 1L, 100L));
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region1)
                .id("id1")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 100L, 200L));
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region2)
                .id("id2")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region3 = new Region(new RangeFactory(schema).createRange(field, 200L, null));
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region3)
                .id("id3")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
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
        assertThat(retrievedPartitions).containsExactly(partition0, partition1, partition2, partition3);
    }

    @Test
    public void shouldReturnLeafPartitions() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition rootPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 1L));
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region1)
                .id("id1")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 1L, null));
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region2)
                .id("id2")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        rootPartition.setLeafPartition(false);
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, partition1, partition2);
        Region region3 = new Region(new RangeFactory(schema).createRange(field, 1L, 9L));
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region3)
                .id("id3")
                .leafPartition(true)
                .parentPartitionId(partition2.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region4 = new Region(new RangeFactory(schema).createRange(field, 9L, null));
        Partition partition4 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region4)
                .id("id4")
                .leafPartition(true)
                .parentPartitionId(partition2.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
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
        assertThat(retrievedPartitions).containsExactly(partition1, partition3, partition4);
    }

    @Test
    public void shouldUpdatePartitions() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);

        // When
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        parentPartition.setDimension(1);
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .dimension(-1)
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .dimension(-1)
                .build();
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);

        // Then
        assertThat(dynamoDBStateStore.getAllPartitions())
                .containsExactlyInAnyOrder(parentPartition, childPartition1, childPartition2);
    }

    @Test
    public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);

        // When / Then
        //  - Attempting to split something that has already been split should fail
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentIsLeaf() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child3", "child2")); // Wrong children
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("notparent") // Wrong parent
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereNewPartitionIsNotLeaf() throws IOException, StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition.setLeafPartition(false);
        parentPartition.setChildPartitionIds(Arrays.asList("child1", "child2"));
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, Long.MAX_VALUE));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(false) // Not leaf
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws StateStoreException, IOException {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws StateStoreException, IOException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(Collections.singletonList(new LongType()))
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws StateStoreException, IOException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, "", null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(Collections.singletonList(new StringType()))
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws StateStoreException, IOException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(Collections.singletonList(new ByteArrayType()))
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }
}
