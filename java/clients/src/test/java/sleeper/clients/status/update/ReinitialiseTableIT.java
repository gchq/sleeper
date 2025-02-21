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
package sleeper.clients.status.update;

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
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
        assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted();
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

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted();
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

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted();
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

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted();
    }

    private void assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted() {
        String tableId = tableProperties.get(TABLE_ID);
        assertThat(s3Client.listObjectsV2(instanceProperties.get(DATA_BUCKET))
                .getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .containsExactlyInAnyOrder(
                        tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_1,
                        tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_2,
                        tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_3);
    }

    private void assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted() {
        String tableId = tableProperties.get(TABLE_ID);
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(instanceProperties.get(DATA_BUCKET));
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        assertThat(result.getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .contains(
                        tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_1,
                        tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_2,
                        tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_3);
        assertThat(result.getKeyCount()).isEqualTo(3);
    }

    private void reinitialiseTableAndDeletePartitions(TableProperties tableProperties) throws IOException {
        new ReinitialiseTable(s3Client,
                dynamoClient, instanceProperties.get(ID), tableProperties.get(TABLE_NAME), true)
                .run();
    }

    private void reinitialiseTable(TableProperties tableProperties) throws IOException {
        new ReinitialiseTable(s3Client,
                dynamoClient, instanceProperties.get(ID), tableProperties.get(TABLE_NAME), false)
                .run();
    }

    private void reinitialiseTableFromSplitPoints(TableProperties tableProperties, String splitPointsFile) throws IOException {
        new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoClient, instanceProperties.get(ID), tableProperties.get(TABLE_NAME), splitPointsFile, false)
                .run();
    }

    private void reinitialiseTableFromSplitPointsEncoded(TableProperties tableProperties, String splitPointsFile) throws IOException {
        new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoClient, instanceProperties.get(ID), tableProperties.get(TABLE_NAME), splitPointsFile, true)
                .run();
    }

    private void saveProperties() {
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        tablePropertiesStore.save(tableProperties);
    }

    private void saveTableDataFiles() {
        String dataBucket = instanceProperties.get(DATA_BUCKET);
        String tableId = tableProperties.get(TABLE_ID);

        s3Client.putObject(dataBucket, tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_1, "some-content");
        s3Client.putObject(dataBucket, tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_2, "some-content");
        s3Client.putObject(dataBucket, tableId + "/" + FILE_SHOULD_NOT_BE_DELETED_3, "some-content");
        s3Client.putObject(dataBucket, tableId + "/partition-root/file1.parquet", "some-content");
        s3Client.putObject(dataBucket, tableId + "/partition-1/file2.parquet", "some-content");
        s3Client.putObject(dataBucket, tableId + "/partition-2/file3.parquet", "some-content");
    }

    private TransactionLogStateStore setupTransactionLogStateStore(TableProperties tableProperties) throws IOException {
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        TransactionLogStateStore transctionLogStateStore = DynamoDBTransactionLogStateStore.builderFrom(
                instanceProperties, tableProperties, dynamoClient, s3Client, hadoopConf).build();

        update(transctionLogStateStore).initialise(KEY_VALUE_SCHEMA);
        setupPartitionsAndAddFiles(transctionLogStateStore);

        return transctionLogStateStore;
    }

    private void setupPartitionsAndAddFiles(StateStore stateStore) throws IOException {
        //  - Get root partition
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = createTempDirectory(tempDir, null).toString();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        String file3 = folderName + "/file3.parquet";

        FileReference fileReference1 = createFileReference(file1, rootPartition.getId());
        FileReference fileReference2 = createFileReference(file2, rootPartition.getId());

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
                fileWithNoReferences(file3)));
    }

    private FileReference createFileReference(String filename, String partitionId) {
        return FileReference.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
    }

    private String createSplitPointsFile(boolean encoded) throws IOException {
        String splitPointsFileName = tempDir.toString() + "/split-points.txt";
        FileWriter fstream = new FileWriter(splitPointsFileName, StandardCharsets.UTF_8);
        BufferedWriter info = new BufferedWriter(fstream);
        if (encoded) {
            info.write(Base64.encodeBase64String((SPLIT_PARTITION_STRING_1.getBytes(StandardCharsets.UTF_8))));
            info.newLine();
            info.write(Base64.encodeBase64String((SPLIT_PARTITION_STRING_2.getBytes(StandardCharsets.UTF_8))));
        } else {
            info.write(SPLIT_PARTITION_STRING_1);
            info.newLine();
            info.write(SPLIT_PARTITION_STRING_2);
        }
        info.close();
        return splitPointsFileName;
    }
}
