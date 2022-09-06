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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class S3StateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

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

    private StateStore getStateStore(Schema schema,
                                     List<Partition> partitions,
                                     int garbageCollectorDelayBeforeDeletionInSeconds) throws IOException, StateStoreException {
        String bucket = folder.newFolder().getAbsolutePath();
        String dynamoTableName = createDynamoTable();
        S3StateStore stateStore = new S3StateStore("", 5, bucket, dynamoTableName, schema, garbageCollectorDelayBeforeDeletionInSeconds, dynamoDBClient, new Configuration());
        stateStore.initialise(partitions);
        return stateStore;
    }

    private StateStore getStateStore(Schema schema,
                                     List<Partition> partitions) throws IOException, StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints, int garbageCollectorDelayBeforeDeletionInSeconds) throws IOException, StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), garbageCollectorDelayBeforeDeletionInSeconds);
    }

    private StateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints) throws IOException, StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), 0);
    }

    private StateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInSeconds) throws IOException, StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.EMPTY_LIST, garbageCollectorDelayBeforeDeletionInSeconds);
    }

    private StateStore getStateStore(Schema schema) throws IOException, StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.EMPTY_LIST);
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
    public void shouldReturnCorrectFileInfoForLongRowKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
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
    public void shouldReturnCorrectFileInfoForByteArrayKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new ByteArrayType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(new byte[]{1}));
        fileInfo.setMaxRowKey(Key.create(new byte[]{10}));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
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
    public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(Arrays.asList(new byte[]{1}, new byte[]{2})));
        fileInfo.setMaxRowKey(Key.create(Arrays.asList(new byte[]{10}, new byte[]{11})));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
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
    public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType(), new StringType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(Arrays.asList(1L, "Z")));
        fileInfo.setMaxRowKey(Key.create(Arrays.asList(10L, "A")));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

        // When
        stateStore.addFile(fileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).singleElement().satisfies(found -> {
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
    public void shouldReturnAllFileInfos() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("" + i);
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(1_000_000L);
            fileInfo.setNumberOfRecords(1L);
            expected.add(fileInfo);
        }
        stateStore.addFiles(new ArrayList<>(expected));

        // When
        List<FileInfo> fileInfos = stateStore.getActiveFiles();

        // Then
        assertThat(fileInfos).hasSize(10000).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingFilename() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

        // When / Then
        assertThatThrownBy(() -> stateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingStatus() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFilename("abc");
        fileInfo.setPartitionId("1");
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

        // When / Then
        assertThatThrownBy(() -> stateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingPartition() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new LongType());
        fileInfo.setFilename("abc");
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setMinRowKey(Key.create(1L));
        fileInfo.setMaxRowKey(Key.create(10L));
        fileInfo.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo.setNumberOfRecords(1L);

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
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file-" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("root");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(1_000_000L);
            fileInfo.setNumberOfRecords(1L);
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
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        int retries = 0;
        while (activeFiles.size() < 20 && retries < 5) {
            Thread.sleep(3000L);
            retries++;
            activeFiles = stateStore.getActiveFiles();
        }
        assertThat(activeFiles).hasSize(20).containsExactlyInAnyOrderElementsOf(files);
        executorService.shutdown();
    }

    @Test
    public void testGetFilesThatAreReadyForGC() throws IOException, InterruptedException, StateStoreException {
        // Given
        Schema schema = schemaWithKeyAndValueWithTypes(new IntType(), new StringType());
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

        // Then 1
        assertThat(readyForGCFilesIterator).toIterable().containsExactly(fileInfo1);

        // When 2
        Thread.sleep(9000L);
        readyForGCFilesIterator = stateStore.getReadyForGCFiles();

        // Then 2
        assertThat(readyForGCFilesIterator).toIterable().containsExactlyInAnyOrder(fileInfo1, fileInfo3);
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobId() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setMinRowKey(Key.create(1L));
        fileInfo1.setMaxRowKey(Key.create(10L));
        fileInfo1.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo1.setNumberOfRecords(1L);
        stateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("2");
        fileInfo2.setMinRowKey(Key.create(20L));
        fileInfo2.setMaxRowKey(Key.create(29L));
        fileInfo2.setLastStateStoreUpdateTime(2_000_000L);
        fileInfo2.setNumberOfRecords(2L);
        stateStore.addFile(fileInfo2);
        FileInfo fileInfo3 = new FileInfo();
        fileInfo3.setRowKeyTypes(new LongType());
        fileInfo3.setFilename("file3");
        fileInfo3.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo3.setPartitionId("3");
        fileInfo3.setJobId("job1");
        fileInfo3.setMinRowKey(Key.create(100L));
        fileInfo3.setMaxRowKey(Key.create(10000L));
        fileInfo3.setLastStateStoreUpdateTime(3_000_000L);
        fileInfo3.setNumberOfRecords(3L);
        stateStore.addFile(fileInfo3);

        // When
        List<FileInfo> fileInfos = stateStore.getActiveFilesWithNoJobId();

        // Then
        assertThat(fileInfos).containsExactly(fileInfo1, fileInfo2);
    }

    @Test
    public void shouldDeleteReadyForGCFile() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("4");
        fileInfo1.setMinRowKey(Key.create(1L));
        fileInfo1.setMaxRowKey(Key.create(10L));
        fileInfo1.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo1.setNumberOfRecords(1L);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo2.setPartitionId("5");
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(10L));
        fileInfo2.setLastStateStoreUpdateTime(2_000_000L);
        fileInfo2.setNumberOfRecords(2L);
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));

        // When
        stateStore.deleteReadyForGCFile(fileInfo2);

        // Then
        assertThat(stateStore.getActiveFiles()).containsExactly(fileInfo1);
        assertThat(stateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldNotDeleteReadyForGCFileIfNotMarkedAsReadyForGC() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename("file1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("4");
        fileInfo1.setMinRowKey(Key.create(1L));
        fileInfo1.setMaxRowKey(Key.create(10L));
        fileInfo1.setLastStateStoreUpdateTime(1_000_000L);
        fileInfo1.setNumberOfRecords(1L);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename("file2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo2.setPartitionId("5");
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(10L));
        fileInfo2.setLastStateStoreUpdateTime(2_000_000L);
        fileInfo2.setNumberOfRecords(2L);
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));

        // When
        assertThatThrownBy(() -> stateStore.deleteReadyForGCFile(fileInfo1))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFile() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
            fileInfo.setNumberOfRecords(1L);
            filesToMoveToReadyForGC.add(fileInfo);
            stateStore.addFile(fileInfo);
        }
        FileInfo newFileInfo = new FileInfo();
        newFileInfo.setRowKeyTypes(new LongType());
        newFileInfo.setFilename("file-new");
        newFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFileInfo.setPartitionId("7");
        newFileInfo.setMinRowKey(Key.create(1L));
        newFileInfo.setMaxRowKey(Key.create(10L));
        newFileInfo.setLastStateStoreUpdateTime(10_000_000L);
        newFileInfo.setNumberOfRecords(4L);

        // When
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).containsExactly(newFileInfo);
        assertThat(stateStore.getReadyForGCFiles()).toIterable().hasSize(4);
    }

    @Test
    public void shouldAtomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesForSplittingJob() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
            fileInfo.setNumberOfRecords((long) i);
            filesToMoveToReadyForGC.add(fileInfo);
        }
        stateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newLeftFileInfo = new FileInfo();
        newLeftFileInfo.setRowKeyTypes(new LongType());
        newLeftFileInfo.setFilename("file-left-new");
        newLeftFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newLeftFileInfo.setPartitionId("7");
        newLeftFileInfo.setMinRowKey(Key.create(1L));
        newLeftFileInfo.setMaxRowKey(Key.create(5L));
        newLeftFileInfo.setLastStateStoreUpdateTime(10_000_000L);
        newLeftFileInfo.setNumberOfRecords(5L);
        FileInfo newRightFileInfo = new FileInfo();
        newRightFileInfo.setRowKeyTypes(new LongType());
        newRightFileInfo.setFilename("file-right-new");
        newRightFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newRightFileInfo.setPartitionId("7");
        newRightFileInfo.setMinRowKey(Key.create(5L));
        newRightFileInfo.setMaxRowKey(Key.create(10L));
        newRightFileInfo.setLastStateStoreUpdateTime(10_000_000L);
        newRightFileInfo.setNumberOfRecords(5L);

        // When
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo);

        // Then
        assertThat(stateStore.getActiveFiles()).containsExactlyInAnyOrder(newLeftFileInfo, newRightFileInfo);
        assertThat(stateStore.getReadyForGCFiles()).toIterable().hasSize(4);
    }

    @Test
    public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFileShouldFailIfFilesNotActive() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<FileInfo> filesToMoveToReadyForGC = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("7");
            fileInfo.setMinRowKey(Key.create(1L));
            fileInfo.setMaxRowKey(Key.create(10L));
            fileInfo.setLastStateStoreUpdateTime(1_000_000L);
            fileInfo.setNumberOfRecords(1L);
            filesToMoveToReadyForGC.add(fileInfo);
        }
        //  - One of the files is not active
        filesToMoveToReadyForGC.get(3).setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        stateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newFileInfo = new FileInfo();
        newFileInfo.setRowKeyTypes(new LongType());
        newFileInfo.setFilename("file-new");
        newFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFileInfo.setPartitionId("7");
        newFileInfo.setMinRowKey(Key.create(1L));
        newFileInfo.setMaxRowKey(Key.create(10L));
        newFileInfo.setLastStateStoreUpdateTime(1_000_000L);
        newFileInfo.setNumberOfRecords(1L);

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToMoveToReadyForGC, newFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void atomicallyUpdateStatusToReadyForGCAndCreateNewActiveFilesShouldFailIfFilesNotActive() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
            fileInfo.setNumberOfRecords((long) i);
            filesToMoveToReadyForGC.add(fileInfo);
        }
        stateStore.addFiles(filesToMoveToReadyForGC);
        FileInfo newLeftFileInfo = new FileInfo();
        newLeftFileInfo.setRowKeyTypes(new LongType());
        newLeftFileInfo.setFilename("file-left-new");
        newLeftFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newLeftFileInfo.setPartitionId("7");
        newLeftFileInfo.setMinRowKey(Key.create(1L));
        newLeftFileInfo.setMaxRowKey(Key.create(5L));
        newLeftFileInfo.setLastStateStoreUpdateTime(10_000_000L);
        newLeftFileInfo.setNumberOfRecords(5L);
        FileInfo newRightFileInfo = new FileInfo();
        newRightFileInfo.setRowKeyTypes(new LongType());
        newRightFileInfo.setFilename("file-right-new");
        newRightFileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newRightFileInfo.setPartitionId("7");
        newRightFileInfo.setMinRowKey(Key.create(5L));
        newRightFileInfo.setMaxRowKey(Key.create(10L));
        newRightFileInfo.setLastStateStoreUpdateTime(10_000_000L);
        newRightFileInfo.setNumberOfRecords(5L);
        //  - One of the files is not active
        filesToMoveToReadyForGC.get(3).setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        stateStore.addFiles(filesToMoveToReadyForGC);

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToMoveToReadyForGC, newLeftFileInfo, newRightFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldAtomicallyUpdateJobStatusOfFiles() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
            fileInfo.setNumberOfRecords(1L);
            files.add(fileInfo);
        }
        stateStore.addFiles(files);
        String jobId = UUID.randomUUID().toString();

        // When
        stateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

        // Then
        assertThat(stateStore.getActiveFiles()).hasSize(4)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId")
                .containsExactlyInAnyOrderElementsOf(files)
                .extracting(FileInfo::getJobId).containsOnly(jobId);
        assertThat(stateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws IOException, StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
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
            fileInfo.setNumberOfRecords(1L);
            files.add(fileInfo);
        }
        stateStore.addFiles(files);
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() ->
                stateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
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
        Partition partition = new Partition(schema.getRowKeyTypes(),
                region, "id", true, "P", new ArrayList<>(), -1);
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
        Partition partition = new Partition(schema.getRowKeyTypes(),
                region, "id", false, "P", new ArrayList<>(), 0);
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
            FileInfo fileInfo = new FileInfo();
            fileInfo.setRowKeyTypes(new LongType());
            fileInfo.setFilename("file" + i);
            fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
            fileInfo.setPartitionId("" + (i % 5));
            fileInfo.setMinRowKey(Key.create((long) i % 5));
            fileInfo.setMaxRowKey(Key.create((long) i % 5));
            fileInfo.setLastStateStoreUpdateTime(1_000_000L);
            fileInfo.setNumberOfRecords((long) i);
            files.add(fileInfo);
        }
        stateStore.addFiles(files);

        // When
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToActiveFilesMap();

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
        Partition partition0 = new Partition(schema.getRowKeyTypes(), region0, "id0", true, "root", new ArrayList<>(), -1);
        Region region1 = new Region(new RangeFactory(schema).createRange(field, 1L, 100L));
        Partition partition1 = new Partition(schema.getRowKeyTypes(), region1, "id1", true, "root", new ArrayList<>(), -1);
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 100L, 200L));
        Partition partition2 = new Partition(schema.getRowKeyTypes(), region2, "id2", true, "root", new ArrayList<>(), -1);
        Region region3 = new Region(new RangeFactory(schema).createRange(field, 200L, null));
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
        Partition partition1 = new Partition(schema.getRowKeyTypes(), region1, "id1", true, rootPartition.getId(), new ArrayList<>(), -1);
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 1L, null));
        Partition partition2 = new Partition(schema.getRowKeyTypes(), region2, "id2", true, rootPartition.getId(), new ArrayList<>(), -1);
        rootPartition.setLeafPartition(false);
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, partition1, partition2);
        Region region3 = new Region(new RangeFactory(schema).createRange(field, 1L, 9L));
        Partition partition3 = new Partition(schema.getRowKeyTypes(), region3, "id3", true, partition2.getId(), new ArrayList<>(), -1);
        Region region4 = new Region(new RangeFactory(schema).createRange(field, 9L, null));
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
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId(parentPartition.getId());
        childPartition1.setDimension(-1);
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId(parentPartition.getId());
        childPartition2.setDimension(-1);
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
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId(parentPartition.getId());
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, null));
        childPartition2.setRegion(region2);

        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId(parentPartition.getId());
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
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("parent");
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

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
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("parent");
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

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
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("notparent"); // Wrong parent
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(true);
        childPartition2.setId("child2");
        Region region2 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

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
        Partition childPartition1 = new Partition();
        childPartition1.setRowKeyTypes(new LongType());
        childPartition1.setLeafPartition(true);
        childPartition1.setId("child1");
        Region region1 = new Region(new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 0L));
        childPartition1.setRegion(region1);
        childPartition1.setChildPartitionIds(new ArrayList<>());
        childPartition1.setParentPartitionId("parent");
        Partition childPartition2 = new Partition();
        childPartition2.setRowKeyTypes(new LongType());
        childPartition2.setLeafPartition(false); // Not leaf
        childPartition2.setId("child2");
        Region region2 = new Region(new RangeFactory(schema).createRange(field, 0L, Long.MAX_VALUE));
        childPartition2.setRegion(region2);
        childPartition2.setChildPartitionIds(new ArrayList<>());
        childPartition2.setParentPartitionId("parent");

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
        Partition expectedPartition = new Partition(schema.getRowKeyTypes(),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
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
        Partition expectedPartition = new Partition(Collections.singletonList(new LongType()),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
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
        Partition expectedPartition = new Partition(Collections.singletonList(new StringType()),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
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
        Partition expectedPartition = new Partition(Collections.singletonList(new ByteArrayType()),
                expectedRegion, partitions.get(0).getId(), true, null, new ArrayList<>(), -1);
        assertThat(partitions).containsExactly(expectedPartition);
    }
}