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
package sleeper.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.statestore.s3.S3StateStore;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ENCRYPTED;
import static sleeper.configuration.properties.table.TableProperty.FILE_IN_PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.FILE_LIFECYCLE_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.statestore.s3.S3StateStore.CURRENT_FILES_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.CURRENT_PARTITIONS_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.CURRENT_REVISION;
import static sleeper.statestore.s3.S3StateStore.REVISION_ID_KEY;

@Testcontainers
public class ReinitialiseTableIT {
    private static final String INSTANCE_NAME = "test";
    private static final String CONFIG_BUCKET_NAME = "sleeper-" + INSTANCE_NAME + "-config";
    private static final String DYNAMO_STATE_STORE_CLASS = "sleeper.statestore.dynamodb.DynamoDBStateStore";
    private static final String S3_STATE_STORE_CLASS = "sleeper.statestore.s3.S3StateStore";
    private static final String FILE_SHOULD_NOT_BE_DELETED_1 = "file0.parquet";
    private static final String FILE_SHOULD_NOT_BE_DELETED_2 = "for_ingest/file0.parquet";
    private static final String FILE_SHOULD_NOT_BE_DELETED_3 = "partition.parquet";
    private static final String SPLIT_PARTITION_STRING_1 = "alpha";
    private static final String SPLIT_PARTITION_STRING_2 = "beta";
    private static final String S3_STATE_STORE_PARTITIONS_FILENAME = "statestore/partitions/file4.parquet";
    private static final String S3_STATE_STORE_FILES_FILENAME = "statestore/files/file5.parquet";
    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

    private static AmazonDynamoDB dynamoDBClient;
    private static AmazonS3 s3Client;

    @BeforeEach
    public void beforeEach() {
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
        s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .build();
    }

    @AfterEach
    public void afterEach() {
        s3Client.shutdown();
        dynamoDBClient.shutdown();
        dynamoDBClient = null;
        s3Client = null;
    }

    @TempDir
    public Path folder;

    @Test
    public void shouldThrowExceptionIfBucketIsEmpty() {
        // Given
        String tableName = UUID.randomUUID().toString();

        // When
        assertThatThrownBy(() -> new ReinitialiseTable(s3Client, dynamoDBClient, "", tableName, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowExceptionIfTableIsEmpty() {
        assertThatThrownBy(() -> new ReinitialiseTable(s3Client, dynamoDBClient, CONFIG_BUCKET_NAME, "", false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldDeleteFileInPartitionAndFileExistenceRecordsByDefaultForDynamoStateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, false);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, false);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, false);
        validTableProperties.saveToS3(s3Client);

        DynamoDBStateStore dynamoStateStore = setupDynamoStateStore(validInstanceProperties, validTableProperties);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, false);
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreFileInPartitionsAndFileExistenceDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        assertThat(dynamoStateStore.getAllPartitions()).hasSize(3);
        assertThat(dynamoStateStore.getLeafPartitions()).hasSize(2);
        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldDeleteFilesInfoAndObjectsInPartitionsByDefaultForS3StateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, true);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, true);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, true);
        validTableProperties.saveToS3(s3Client);

        S3StateStore s3StateStore = setupS3StateStore(validInstanceProperties, validTableProperties);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, false);
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "2");
        List<FileInfo> fileInPartitionList = s3StateStore.getFileInPartitionList()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(fileInPartitionList).isEmpty();
        assertThat(s3StateStore.getAllPartitions()).hasSize(3);
        assertThat(s3StateStore.getLeafPartitions()).hasSize(2);
        assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldDeletePartitionsWhenOptionSelectedForDynamoStateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, false);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, false);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, false);
        validTableProperties.saveToS3(s3Client);

        DynamoDBStateStore dynamoStateStore = setupDynamoStateStore(validInstanceProperties, validTableProperties);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true);
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreFileInPartitionsAndFileExistenceDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(1);
        assertThat(dynamoStateStore.getLeafPartitions()).hasSize(1);
        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldDeletePartitionsWhenOptionSelectedForS3StateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, true);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, true);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, true);
        validTableProperties.saveToS3(s3Client);

        S3StateStore s3StateStore = setupS3StateStore(validInstanceProperties, validTableProperties);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true);
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "1");

        List<FileInfo> fileInPartitionList = s3StateStore.getFileInPartitionList()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(fileInPartitionList).isEmpty();
        assertThat(s3StateStore.getAllPartitions()).hasSize(1);
        assertThat(s3StateStore.getLeafPartitions()).hasSize(1);

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldSetUpSplitPointsFromFileWhenOptionSelectedForDynamoStateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, false);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, false);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, false);
        validTableProperties.saveToS3(s3Client);

        DynamoDBStateStore dynamoStateStore = setupDynamoStateStore(validInstanceProperties, validTableProperties);

        String splitPointsFileName = createSplitPointsFile(false);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, false);
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreFileInPartitionsAndFileExistenceDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(5);
        assertThat(dynamoStateStore.getLeafPartitions()).hasSize(3);
        assertThat(partitionsList)
                .extracting(partition -> partition.getRegion().getRange("key").getMin().toString())
                .contains(SPLIT_PARTITION_STRING_1, SPLIT_PARTITION_STRING_2);

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldSetUpSplitPointsFromFileWhenOptionSelectedForS3StateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, true);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, true);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, true);
        validTableProperties.saveToS3(s3Client);

        S3StateStore s3StateStore = setupS3StateStore(validInstanceProperties, validTableProperties);

        String splitPointsFileName = createSplitPointsFile(false);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, false);
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "1");

        List<FileInfo> fileInPartitionList = s3StateStore.getFileInPartitionList()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(fileInPartitionList).isEmpty();

        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(5);
        assertThat(s3StateStore.getLeafPartitions()).hasSize(3);
        assertThat(partitionsList)
                .extracting(partition -> partition.getRegion().getRange("key").getMin().toString())
                .contains(SPLIT_PARTITION_STRING_1, SPLIT_PARTITION_STRING_2);

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldHandleEncodedSplitPointsFileWhenOptionSelectedForDynamoStateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, false);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, false);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, false);
        validTableProperties.saveToS3(s3Client);

        DynamoDBStateStore dynamoStateStore = setupDynamoStateStore(validInstanceProperties, validTableProperties);

        String splitPointsFileName = createSplitPointsFile(true);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, true);
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreFileInPartitionsAndFileExistenceDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(5);
        assertThat(dynamoStateStore.getLeafPartitions()).hasSize(3);
        assertThat(partitionsList)
                .extracting(partition -> partition.getRegion().getRange("key").getMin().toString())
                .contains(SPLIT_PARTITION_STRING_1, SPLIT_PARTITION_STRING_2);

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldHandleEncodedSplitPointsFileWhenOptionSelectedForS3StateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, true);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, true);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, true);
        validTableProperties.saveToS3(s3Client);

        S3StateStore s3StateStore = setupS3StateStore(validInstanceProperties, validTableProperties);

        String splitPointsFileName = createSplitPointsFile(true);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, true);
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "1");

        List<FileInfo> fileInPartitionList = s3StateStore.getFileInPartitionList()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(fileInPartitionList).isEmpty();

        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(5);
        assertThat(s3StateStore.getLeafPartitions()).hasSize(3);
        assertThat(partitionsList)
                .extracting(partition -> partition.getRegion().getRange("key").getMin().toString())
                .contains(SPLIT_PARTITION_STRING_1, SPLIT_PARTITION_STRING_2);

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldIgnoreSplitFileAndEncodedOptionsIfDeletePartitionsFalseForDynamoStateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, false);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, false);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, false);
        validTableProperties.saveToS3(s3Client);

        DynamoDBStateStore dynamoStateStore = setupDynamoStateStore(validInstanceProperties, validTableProperties);

        String splitPointsFileName = createSplitPointsFile(false);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, false,
                splitPointsFileName, true);
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreFileInPartitionsAndFileExistenceDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(3);
        assertThat(dynamoStateStore.getLeafPartitions()).hasSize(2);

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldIgnoreSplitFileAndEncryptedOptionsIfDeletePartitionsFalseForS3StateStore() throws Exception {
        // Given
        String tableName = UUID.randomUUID().toString();
        String tableBucketName = "sleeper" + "-" + INSTANCE_NAME + "-table-" + tableName;
        setupS3buckets(tableBucketName, true);

        InstanceProperties validInstanceProperties =
                createValidInstanceProperties(tableName, true);
        validInstanceProperties.saveToS3(s3Client);

        TableProperties validTableProperties =
                createValidTableProperties(validInstanceProperties, tableName, true);
        validTableProperties.saveToS3(s3Client);

        S3StateStore s3StateStore = setupS3StateStore(validInstanceProperties, validTableProperties);

        String splitPointsFileName = createSplitPointsFile(false);

        // When
        ReinitialiseTable reinitialiseTable = new ReinitialiseTableFromSplitPoints(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, false,
                splitPointsFileName, true);
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "2");

        List<FileInfo> fileInPartitionList = s3StateStore.getFileInPartitionList()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(fileInPartitionList).isEmpty();

        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(3);
        assertThat(s3StateStore.getLeafPartitions()).hasSize(2);

        assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    private void assertDynamoStateStoreFileInPartitionsAndFileExistenceDynamoTablesAreNowEmpty(
            TableProperties tableProperties, DynamoDBStateStore dynamoStateStore) throws StateStoreException {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(FILE_LIFECYCLE_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertThat(scanResult.getItems()).isEmpty();
        assertThat(dynamoStateStore.getFileInPartitionList()).isEmpty();
    }

    private void assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(TableProperties tableProperties,
                                                                             String expectedFilesVersion,
                                                                             String expectedPartitionsVersion) {
        // - The revisions file should have two entries one for partitions and one for files and both should now be
        //   set to version 00000000000001
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(REVISION_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertThat(scanResult.getItems()).hasSize(2);
        String filesVersion = "";
        String partitionsVersion = "";
        String versionPrefix = "00000000000";
        for (Map<String, AttributeValue> item : scanResult.getItems()) {
            if (item.get(REVISION_ID_KEY).toString().contains(CURRENT_FILES_REVISION_ID_KEY)) {
                filesVersion = item.get(CURRENT_REVISION).toString();
            }
            if (item.get(REVISION_ID_KEY).toString().contains(CURRENT_PARTITIONS_REVISION_ID_KEY)) {
                partitionsVersion = item.get(CURRENT_REVISION).toString();
            }
        }

        assertThat(filesVersion).isNotEmpty().contains(versionPrefix + expectedFilesVersion);
        assertThat(partitionsVersion).isNotEmpty().contains(versionPrefix + expectedPartitionsVersion);
    }

    private void assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(String tableBucketName) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(tableBucketName).withMaxKeys(10);
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        assertThat(result.getKeyCount()).isEqualTo(3);
        assertThat(result.getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .contains(FILE_SHOULD_NOT_BE_DELETED_1, FILE_SHOULD_NOT_BE_DELETED_2, FILE_SHOULD_NOT_BE_DELETED_3);
    }

    private void assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted(
            String tableBucketName) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(tableBucketName).withMaxKeys(10);
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        assertThat(result.getKeyCount()).isEqualTo(4);
        assertThat(result.getObjectSummaries())
                .extracting(S3ObjectSummary::getKey)
                .contains(FILE_SHOULD_NOT_BE_DELETED_1, FILE_SHOULD_NOT_BE_DELETED_2, FILE_SHOULD_NOT_BE_DELETED_3,
                        S3_STATE_STORE_PARTITIONS_FILENAME)
                .doesNotContain(S3_STATE_STORE_FILES_FILENAME);
    }

    private void setupS3buckets(String tableBucketName, boolean isS3StateStore) {
        s3Client.createBucket(CONFIG_BUCKET_NAME);
        s3Client.createBucket(tableBucketName);

        s3Client.putObject(tableBucketName, FILE_SHOULD_NOT_BE_DELETED_1, "some-content");
        s3Client.putObject(tableBucketName, FILE_SHOULD_NOT_BE_DELETED_2, "some-content");
        s3Client.putObject(tableBucketName, FILE_SHOULD_NOT_BE_DELETED_3, "some-content");
        s3Client.putObject(tableBucketName, "partition-root/file1.parquet", "some-content");
        s3Client.putObject(tableBucketName, "partition-1/file2.parquet", "some-content");
        s3Client.putObject(tableBucketName, "partition-2/file3.parquet", "some-content");

        if (isS3StateStore) {
            s3Client.putObject(tableBucketName, S3_STATE_STORE_FILES_FILENAME, "some-content");
            s3Client.putObject(tableBucketName, S3_STATE_STORE_PARTITIONS_FILENAME, "some-content");
        }
    }

    private DynamoDBStateStore setupDynamoStateStore(InstanceProperties instanceProperties, TableProperties tableProperties)
            throws IOException, StateStoreException {
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator =
                new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDBClient);
        DynamoDBStateStore dynamoDBStateStore = dynamoDBStateStoreCreator.create();

        dynamoDBStateStore.initialise();

        setupPartitionsAndAddFileInfo(dynamoDBStateStore);

        // - Check DynamoDBStateStore is set up correctly
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(FILE_LIFECYCLE_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertThat(scanResult.getItems()).hasSize(3);

        // - Check DynamoDBStateStore has correct file in partition list
        List<FileInfo> fileInPartitionList = dynamoDBStateStore.getFileInPartitionList()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(fileInPartitionList).hasSize(3);

        // - Check DynamoDBStateStore has correct partitions
        List<Partition> partitionsList = dynamoDBStateStore.getAllPartitions();
        assertThat(partitionsList).hasSize(3);

        return dynamoDBStateStore;
    }

    private S3StateStore setupS3StateStore(InstanceProperties instanceProperties, TableProperties tableProperties)
            throws IOException, StateStoreException {
        //  - CreateS3StateStore
        String revisionTableName = createRevisionDynamoTable(tableProperties.get(REVISION_TABLENAME));
        S3StateStore s3StateStore = new S3StateStore(instanceProperties.get(FILE_SYSTEM), 5,
                tableProperties.get(DATA_BUCKET), revisionTableName,
                tableProperties.getSchema(), 600,
                dynamoDBClient, new Configuration());
        s3StateStore.initialise();

        setupPartitionsAndAddFileInfo(s3StateStore);

        // - Check S3StateStore is set up correctly
        // - The revisions file should have two entries one for partitions and one for files and both should now be
        //   set to version 2
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(tableProperties, "2", "2");

        // - Check S3StateStore has correct file in partition list
        assertThat(s3StateStore.getFileInPartitionList()).hasSize(3);

        // - Check S3StateStore has correct partitions
        assertThat(s3StateStore.getAllPartitions()).hasSize(3);

        return s3StateStore;
    }

    private void setupPartitionsAndAddFileInfo(StateStore stateStore) throws IOException, StateStoreException {
        //  - Get root partition
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = createTempDirectory(folder, null).toString();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        String file3 = folderName + "/file3.parquet";

        FileInfo fileInfo1 = createFileInfo(file1, FileInfo.FileStatus.ACTIVE, rootPartition.getId(),
                Key.create("0"), Key.create("98"));
        FileInfo fileInfo2 = createFileInfo(file2, FileInfo.FileStatus.ACTIVE, rootPartition.getId(),
                Key.create("1"), Key.create("9"));
        FileInfo fileInfo3 = createFileInfo(file3, FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION, rootPartition.getId(),
                Key.create("1"), Key.create("9"));

        //  - Split root partition
        rootPartition.setLeafPartition(false);
        Range leftRange = new RangeFactory(KEY_VALUE_SCHEMA).createRange(KEY_VALUE_SCHEMA.getRowKeyFields().get(0), "0", "eee");
        Region leftRegion = new Region(leftRange);
        Partition leftPartition = Partition.builder()
                .leafPartition(true)
                .region(leftRegion)
                .id("0" + "---eee")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        Range rightRange = new RangeFactory(KEY_VALUE_SCHEMA).createRange(KEY_VALUE_SCHEMA.getRowKeyFields().get(0), "eee", "zzz");
        Region rightRegion = new Region(rightRange);
        Partition rightPartition = Partition.builder()
                .leafPartition(true)
                .region(rightRegion)
                .id("eee---zzz")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);

        //  - Update Dynamo state store with details of files
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2, fileInfo3));
    }

    private FileInfo createFileInfo(String filename, FileInfo.FileStatus fileStatus, String partitionId,
                                    Key minRowKey, Key maxRowKey) {
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new StringType())
                .filename(filename)
                .fileStatus(fileStatus)
                .partitionId(partitionId)
                .numberOfRecords(100L)
                .minRowKey(minRowKey)
                .maxRowKey(maxRowKey)
                .lastStateStoreUpdateTime(100L)
                .build();

        return fileInfo;
    }

    private InstanceProperties createValidInstanceProperties(String tableName, boolean isS3StateStore) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, INSTANCE_NAME);
        instanceProperties.set(ACCOUNT, "1234567890");
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        instanceProperties.set(JARS_BUCKET, "bucket");
        instanceProperties.set(SUBNET, "subnet1");
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.setNumber(LOG_RETENTION_IN_DAYS, 1);
        if (isS3StateStore) {
            instanceProperties.set(FILE_SYSTEM, "");
        } else {
            instanceProperties.set(FILE_SYSTEM, "s3a://");
        }

        return instanceProperties;
    }

    private TableProperties createValidTableProperties(InstanceProperties instanceProperties, String tableName,
                                                       boolean isS3StateStore) throws IOException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(KEY_VALUE_SCHEMA);
        tableProperties.set(ENCRYPTED, "false");
        tableProperties.set(FILE_IN_PARTITION_TABLENAME, "sleeper" + "-" + tableName + "-" + "file-in-partition");
        tableProperties.set(FILE_LIFECYCLE_TABLENAME, "sleeper" + "-" + tableName + "-" + "gc-files");
        tableProperties.set(PARTITION_TABLENAME, "sleeper" + "-" + tableName + "-" + "partitions");
        tableProperties.set(REVISION_TABLENAME, "sleeper" + "-" + tableName + "-" + "revisions");
        if (isS3StateStore) {
            tableProperties.set(TableProperty.STATESTORE_CLASSNAME, S3_STATE_STORE_CLASS);
            tableProperties.set(DATA_BUCKET, createTempDirectory(folder, null).toString());
        } else {
            tableProperties.set(TableProperty.STATESTORE_CLASSNAME, DYNAMO_STATE_STORE_CLASS);
        }
        return tableProperties;
    }

    private String createSplitPointsFile(boolean encoded) throws IOException {
        String folderName = createTempDirectory(folder, null).toString();
        String splitPointsFileName = folderName + "/split-points.txt";
        FileWriter fstream = new FileWriter(splitPointsFileName);
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

    private String createRevisionDynamoTable(String tableName) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition(REVISION_ID_KEY, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(REVISION_ID_KEY, KeyType.HASH));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        dynamoDBClient.createTable(request);
        return tableName;
    }
}
