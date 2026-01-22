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
package sleeper.clients.table;

import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.clients.table.partition.ReinitialiseTableFromSplitPoints;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class ReinitialiseTableIT extends LocalStackTestBase {
    private static final String FILE_SHOULD_NOT_BE_DELETED_1 = "file0.parquet";
    private static final String FILE_SHOULD_NOT_BE_DELETED_2 = "for_ingest/file0.parquet";
    private static final String FILE_SHOULD_NOT_BE_DELETED_3 = "partition.parquet";
    private static final String FILE_SHOULD_BE_DELETED_1 = "partition-root/file1.parquet";
    private static final String FILE_SHOULD_BE_DELETED_2 = "partition-1/file2.parquet";
    private static final String FILE_SHOULD_BE_DELETED_3 = "partition-2/file3.parquet";
    private static final String SPLIT_PARTITION_STRING_1 = "alpha";
    private static final String SPLIT_PARTITION_STRING_2 = "beta";

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);

    @TempDir
    public Path tempDir;

    @BeforeEach
    public void beforeEach() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    @Test
    public void shouldThrowExceptionIfInstanceIdIsEmpty() {
        // Given
        String tableName = UUID.randomUUID().toString();

        // When
        assertThatThrownBy(() -> new ReinitialiseTable(s3Client, dynamoClient, "", tableName, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsEmpty() {
        assertThatThrownBy(() -> new ReinitialiseTable(s3Client, dynamoClient, instanceProperties.get(ID), "", false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldDeleteFilesInfoAndObjectsInPartitionsByDefault() throws Exception {
        // Given
        saveProperties();
        saveTableDataFiles();
        TransactionLogStateStore stateStore = setupTransactionLogStateStore(tableProperties);

        // When
        reinitialiseTable(tableProperties);

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        assertThat(stateStore.getAllPartitions()).hasSize(3);
        assertThat(stateStore.getLeafPartitions()).hasSize(2);
        assertOnlyExpectedObjectsHaveBeenDeleted();
    }

    @Test
    public void shouldNotDeleteTransationFilesAndSnapshots() throws Exception {
        // Given
        saveProperties();
        saveTableDataFiles();
        //Store Transaction
        TransactionLogStateStore stateStore = setupTransactionLogStateStore(tableProperties);
        S3TransactionBodyStore s3TransactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties), 0);
        PartitionTransaction transaction = new InitialisePartitionsTransaction(
                new PartitionsBuilder(tableProperties).singlePartition("root").buildList());
        String transactionObjectKey = s3TransactionBodyStore.storeIfTooBig(tableProperties.get(TABLE_ID), transaction).getBodyKey().orElseThrow();
        //Store Snapshot
        DynamoDBTransactionLogSnapshotCreator.from(instanceProperties, tableProperties, s3Client, s3TransferManager, dynamoClient)
                .createSnapshot();

        // When
        reinitialiseTable(tableProperties);

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        assertThat(stateStore.getAllPartitions()).hasSize(3);
        assertThat(stateStore.getLeafPartitions()).hasSize(2);

        List<String> list = new ArrayList<>();
        list.add(transactionObjectKey);
        LatestSnapshots snapshot = new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, tableProperties, dynamoClient).getLatestSnapshots();
        list.add(snapshot.getFilesSnapshot().get().getObjectKey());
        list.add(snapshot.getPartitionsSnapshot().get().getObjectKey());
        assertOnlyExpectedObjectsHaveBeenDeleted(list);
    }

    @Test
    public void shouldDeletePartitionsWhenOptionSelected() throws Exception {
        // Given
        saveProperties();
        saveTableDataFiles();
        TransactionLogStateStore stateStore = setupTransactionLogStateStore(tableProperties);

        // When
        reinitialiseTableAndDeletePartitions(tableProperties);

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();
        assertThat(stateStore.getAllPartitions()).hasSize(1);
        assertThat(stateStore.getLeafPartitions()).hasSize(1);

        assertOnlyExpectedObjectsHaveBeenDeleted();
    }

    @Test
    public void shouldSetUpSplitPointsFromFileWhenOptionSelected() throws Exception {
        // Given
        saveProperties();
        saveTableDataFiles();
        TransactionLogStateStore stateStore = setupTransactionLogStateStore(tableProperties);
        String splitPointsFileName = createSplitPointsFile(false);

        // When
        reinitialiseTableFromSplitPoints(tableProperties, splitPointsFileName);

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();

        List<Partition> partitionsList = stateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(5);
        assertThat(stateStore.getLeafPartitions()).hasSize(3);
        assertThat(partitionsList)
                .extracting(partition -> partition.getRegion().getRange("key").getMin().toString())
                .contains(SPLIT_PARTITION_STRING_1, SPLIT_PARTITION_STRING_2);

        assertOnlyExpectedObjectsHaveBeenDeleted();
    }

    @Test
    public void shouldHandleEncodedSplitPointsFileWhenOptionSelected() throws Exception {
        // Given
        saveProperties();
        saveTableDataFiles();
        TransactionLogStateStore stateStore = setupTransactionLogStateStore(tableProperties);
        String splitPointsFileName = createSplitPointsFile(true);

        // When
        reinitialiseTableFromSplitPointsEncoded(tableProperties, splitPointsFileName);

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE))).isEmpty();

        List<Partition> partitionsList = stateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(5);
        assertThat(stateStore.getLeafPartitions()).hasSize(3);
        assertThat(partitionsList)
                .extracting(partition -> partition.getRegion().getRange("key").getMin().toString())
                .contains(SPLIT_PARTITION_STRING_1, SPLIT_PARTITION_STRING_2);

        assertOnlyExpectedObjectsHaveBeenDeleted();
    }

    private void assertOnlyExpectedObjectsHaveBeenDeleted() {
        assertOnlyExpectedObjectsHaveBeenDeleted(new ArrayList<>());
    }

    private void assertOnlyExpectedObjectsHaveBeenDeleted(List<String> objectKeys) {
        String tableId = tableProperties.get(TABLE_ID);
        objectKeys.add(tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_1);
        objectKeys.add(tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_2);
        objectKeys.add(tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_3);
        assertThat(listDataBucketObjectKeys())
                .containsExactlyInAnyOrder(objectKeys.toArray(new String[0]));
    }

    private List<String> listDataBucketObjectKeys() {
        return s3Client.listObjects(ListObjectsRequest.builder()
                .bucket(instanceProperties.get(DATA_BUCKET))
                .build())
                .contents().stream()
                .map(S3Object::key)
                .toList();
    }

    private void reinitialiseTableAndDeletePartitions(TableProperties tableProperties) throws IOException {
        new ReinitialiseTable(s3Client, dynamoClient,
                instanceProperties.get(ID), tableProperties.get(TABLE_NAME), true)
                .run();
    }

    private void reinitialiseTable(TableProperties tableProperties) throws IOException {
        new ReinitialiseTable(s3Client, dynamoClient,
                instanceProperties.get(ID), tableProperties.get(TABLE_NAME), false)
                .run();
    }

    private void reinitialiseTableFromSplitPoints(TableProperties tableProperties, String splitPointsFile) throws IOException {
        new ReinitialiseTableFromSplitPoints(s3Client, dynamoClient,
                instanceProperties.get(ID), tableProperties.get(TABLE_NAME), splitPointsFile, false)
                .run();
    }

    private void reinitialiseTableFromSplitPointsEncoded(TableProperties tableProperties, String splitPointsFile) throws IOException {
        new ReinitialiseTableFromSplitPoints(s3Client, dynamoClient,
                instanceProperties.get(ID), tableProperties.get(TABLE_NAME), splitPointsFile, true)
                .run();
    }

    private void saveProperties() {
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        tablePropertiesStore.save(tableProperties);
    }

    private void saveTableDataFiles() {
        String dataBucket = instanceProperties.get(DATA_BUCKET);
        String tableId = tableProperties.get(TABLE_ID);
        RequestBody requestBody = RequestBody.fromString("some-content");

        for (String file : List.of(FILE_SHOULD_NOT_BE_DELETED_1, FILE_SHOULD_NOT_BE_DELETED_2, FILE_SHOULD_NOT_BE_DELETED_3)) {
            s3Client.putObject(request -> request.bucket(dataBucket).key(tableId + "/" + file), requestBody);
        }

        for (String file : List.of(FILE_SHOULD_BE_DELETED_1, FILE_SHOULD_BE_DELETED_2, FILE_SHOULD_BE_DELETED_3)) {
            s3Client.putObject(request -> request.bucket(dataBucket).key(tableId + "/" + file), requestBody);
            s3Client.putObject(request -> request.bucket(dataBucket).key(tableId + "/" + file.replace(".parquet", ".sketches")), requestBody);
        }
    }

    private TransactionLogStateStore setupTransactionLogStateStore(TableProperties tableProperties) throws IOException {
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        TransactionLogStateStore transctionLogStateStore = DynamoDBTransactionLogStateStore.builderFrom(
                instanceProperties, tableProperties, dynamoClient, s3Client).build();

        update(transctionLogStateStore).initialise(tableProperties.getSchema());
        setupPartitionsAndAddFiles(transctionLogStateStore);

        return transctionLogStateStore;
    }

    private void setupPartitionsAndAddFiles(StateStore stateStore) throws IOException {
        //  - Get root partition
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String tableId = tableProperties.get(TABLE_ID);
        String dataBucket = instanceProperties.get(DATA_BUCKET);
        FileReference fileReference1 = createFileReference("s3a://" + dataBucket + "/" + tableId + "/" + FILE_SHOULD_BE_DELETED_1, rootPartition.getId());
        FileReference fileReference2 = createFileReference(dataBucket + "/" + tableId + "/" + FILE_SHOULD_BE_DELETED_2, rootPartition.getId());

        //  - Split root partition
        PartitionTree tree = new PartitionsBuilder(KEY_VALUE_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "0" + "---eee", "eee---zzz", "eee")
                .buildTree();

        update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(
                tree.getPartition("root"), tree.getPartition("0" + "---eee"), tree.getPartition("eee---zzz"));

        //  - Update Dynamo state store with details of files
        update(stateStore).addFilesWithReferences(List.of(
                fileWithReferences(List.of(fileReference1)),
                fileWithReferences(List.of(fileReference2)),
                fileWithNoReferences(dataBucket + "/" + tableId + "/" + FILE_SHOULD_BE_DELETED_3)));
    }

    private FileReference createFileReference(String filename, String partitionId) {
        return FileReference.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRows(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
    }

    private String createSplitPointsFile(boolean encoded) throws IOException {
        Path path = tempDir.resolve("test-split-points.txt");
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            if (encoded) {
                writer.write(Base64.encodeBase64String(SPLIT_PARTITION_STRING_1.getBytes(StandardCharsets.UTF_8)));
                writer.newLine();
                writer.write(Base64.encodeBase64String(SPLIT_PARTITION_STRING_2.getBytes(StandardCharsets.UTF_8)));
            } else {
                writer.write(SPLIT_PARTITION_STRING_1);
                writer.newLine();
                writer.write(SPLIT_PARTITION_STRING_2);
            }
        }
        return path.toString();
    }
}
