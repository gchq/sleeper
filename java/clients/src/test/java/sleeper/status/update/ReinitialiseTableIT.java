package sleeper.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;
import sleeper.configuration.properties.table.TableProperties;
import static sleeper.configuration.properties.table.TableProperties.TABLES_PREFIX;
import sleeper.configuration.properties.table.TableProperty;
import static sleeper.configuration.properties.table.TableProperty.*;
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
import static sleeper.statestore.s3.S3StateStore.*;

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
    private static final Schema KEY_VALUE_SCHEMA = new Schema();
    static {
        KEY_VALUE_SCHEMA.setRowKeyFields(new Field("key", new StringType()));
        KEY_VALUE_SCHEMA.setValueFields(new Field("value1", new StringType()), new Field("value2", new StringType()));
    }

    @ClassRule
    public static LocalStackContainer localStackContainer =new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

    private static AmazonDynamoDB dynamoDBClient;
    private static AmazonS3 s3Client;

    @Before
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

    @After
    public void afterEach() {
        s3Client.shutdown();
        dynamoDBClient.shutdown();
        dynamoDBClient = null;
        s3Client = null;
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldThrowExceptionIfBucketIsEmpty() {
        // Given
        String tableName = UUID.randomUUID().toString();

        // When
        assertThrows(IllegalArgumentException.class, () -> new ReinitialiseTable(s3Client,
                dynamoDBClient, "", tableName, false,
                null, false));
    }

    @Test
    public void shouldThrowExceptionIfTableIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new ReinitialiseTable(s3Client,
                dynamoDBClient, CONFIG_BUCKET_NAME, "", false,
                null, false));
    }

    @Test
    public void shouldDeleteActiveAndGCFilesByDefaultForDynamoStateStore() throws Exception {
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
                dynamoDBClient, INSTANCE_NAME, tableName, false,
                null, false );
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreActiveFilesAndGCFilesDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        assertEquals(3, dynamoStateStore.getAllPartitions().size());
        assertEquals(2, dynamoStateStore.getLeafPartitions().size());
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
                dynamoDBClient, INSTANCE_NAME, tableName, false,
                null, false );
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "2");
        List<FileInfo> activeFiles = s3StateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(0, activeFiles.size());
        assertEquals(3, s3StateStore.getAllPartitions().size());
        assertEquals(2, s3StateStore.getLeafPartitions().size());
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
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                null, false );
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreActiveFilesAndGCFilesDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertEquals(1, partitionsList.size());
        assertEquals(1, dynamoStateStore.getLeafPartitions().size());
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
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                null, false );
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "1");

        List<FileInfo> activeFiles = s3StateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(0, activeFiles.size());
        assertEquals(1, s3StateStore.getAllPartitions().size());
        assertEquals(1, s3StateStore.getLeafPartitions().size());

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
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, false );
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreActiveFilesAndGCFilesDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertEquals(5, partitionsList.size());
        assertEquals(3, dynamoStateStore.getLeafPartitions().size());
        List<String> s3Keys = partitionsList
                .stream()
                .map(partition -> partition.getRegion().getRange("key").getMin().toString())
                .collect(Collectors.toList());
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_1));
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_2));

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
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, false );
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "1");

        List<FileInfo> activeFiles = s3StateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(0, activeFiles.size());

        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertEquals(5, partitionsList.size());
        assertEquals(3, s3StateStore.getLeafPartitions().size());
        List<String> s3Keys = partitionsList
                .stream()
                .map(partition -> partition.getRegion().getRange("key").getMin().toString())
                .collect(Collectors.toList());
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_1));
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_2));

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
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, true );
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreActiveFilesAndGCFilesDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertEquals(5, partitionsList.size());
        assertEquals(3, dynamoStateStore.getLeafPartitions().size());
        List<String> s3Keys = partitionsList
                .stream()
                .map(partition -> partition.getRegion().getRange("key").getMin().toString())
                .collect(Collectors.toList());
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_1));
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_2));

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
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, true,
                splitPointsFileName, true );
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "1");

        List<FileInfo> activeFiles = s3StateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(0, activeFiles.size());

        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertEquals(5, partitionsList.size());
        assertEquals(3, s3StateStore.getLeafPartitions().size());
        List<String> s3Keys = partitionsList
                .stream()
                .map(partition -> partition.getRegion().getRange("key").getMin().toString())
                .collect(Collectors.toList());
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_1));
        assertTrue(s3Keys.contains(SPLIT_PARTITION_STRING_2));

        assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    @Test
    public void shouldIgnoreSplitFileAndEncryptedOptionsIfDeletePartitionsFalseForDynamoStateStore() throws Exception {
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
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, false,
                splitPointsFileName, true );
        reinitialiseTable.run();

        // Then
        assertDynamoStateStoreActiveFilesAndGCFilesDynamoTablesAreNowEmpty(validTableProperties, dynamoStateStore);
        List<Partition> partitionsList = dynamoStateStore.getAllPartitions();
        assertEquals(3, partitionsList.size());
        assertEquals(2, dynamoStateStore.getLeafPartitions().size());

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
        ReinitialiseTable reinitialiseTable = new ReinitialiseTable(s3Client,
                dynamoDBClient, INSTANCE_NAME, tableName, false,
                splitPointsFileName, true );
        reinitialiseTable.run();

        // Then
        assertS3StateStoreRevisionsDynamoTableNowHasCorrectVersions(
                validTableProperties, "1", "2");

        List<FileInfo> activeFiles = s3StateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(0, activeFiles.size());

        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertEquals(3, partitionsList.size());
        assertEquals(2, s3StateStore.getLeafPartitions().size());

        assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted(tableBucketName);
    }

    private void assertDynamoStateStoreActiveFilesAndGCFilesDynamoTablesAreNowEmpty(
            TableProperties tableProperties, DynamoDBStateStore dynamoStateStore) throws StateStoreException {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertEquals(0, scanResult.getItems().size());

        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(0, activeFiles.size());
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
        assertEquals(2, scanResult.getItems().size());
        String filesVersion = "";
        String partitionsVersion = "";
        String versionPrefix = "00000000000";
        for (Map<String, AttributeValue> item:scanResult.getItems()) {
            if (item.get(REVISION_ID_KEY).toString().contains(CURRENT_FILES_REVISION_ID_KEY)) {
                filesVersion = item.get(CURRENT_REVISION).toString();
            }
            if (item.get(REVISION_ID_KEY).toString().contains(CURRENT_PARTITIONS_REVISION_ID_KEY)) {
                partitionsVersion = item.get(CURRENT_REVISION).toString();
            }
        }

        assertFalse(filesVersion.isEmpty());
        assertFalse(partitionsVersion.isEmpty());
        assertTrue(filesVersion.contains(versionPrefix + expectedFilesVersion));
        assertTrue(partitionsVersion.contains(versionPrefix + expectedPartitionsVersion));
    }

    private void assertObjectsWithinPartitionsAndStateStoreAreaInTheTableBucketHaveBeenDeleted(String tableBucketName) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(tableBucketName).withMaxKeys(10);
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        assertEquals(3, result.getKeyCount());
        List<String> s3Keys = result.getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
        assertTrue(s3Keys.contains(FILE_SHOULD_NOT_BE_DELETED_1));
        assertTrue(s3Keys.contains(FILE_SHOULD_NOT_BE_DELETED_2));
        assertTrue(s3Keys.contains(FILE_SHOULD_NOT_BE_DELETED_3));
    }

    private void assertOnlyObjectsWithinPartitionsAndStateStoreFilesAreasInTheTableBucketHaveBeenDeleted(
            String tableBucketName) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(tableBucketName).withMaxKeys(10);
        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        assertEquals(4, result.getKeyCount());
        List<String> s3Keys = result.getObjectSummaries()
                .stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
        assertTrue(s3Keys.contains(FILE_SHOULD_NOT_BE_DELETED_1));
        assertTrue(s3Keys.contains(FILE_SHOULD_NOT_BE_DELETED_2));
        assertTrue(s3Keys.contains(FILE_SHOULD_NOT_BE_DELETED_3));
        assertTrue(s3Keys.contains(S3_STATE_STORE_PARTITIONS_FILENAME));
        assertFalse(s3Keys.contains(S3_STATE_STORE_FILES_FILENAME));
    }

    private void setupS3buckets (String tableBucketName, boolean isS3StateStore) {
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
        // - The ready for GC table should have 1 item in (but it's not returned by getReadyForGCFiles()
        //   because it is less than 10 seconds since it was marked as ready for GC). As the StateStore API
        //   does not have a method to return all values in the ready for gc table, we query the table
        //   directly.
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertEquals(1, scanResult.getItems().size());

        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoDBStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(2, activeFiles.size());

        // - Check DynamoDBStateStore has correct partitions
        List<Partition> partitionsList = dynamoDBStateStore.getAllPartitions();
        assertEquals(3, partitionsList.size());

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

        // - Check S3StateStore has correct active files
        List<FileInfo> activeFiles = s3StateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertEquals(2, activeFiles.size());

        // - Check S3StateStore has correct partitions
        List<Partition> partitionsList = s3StateStore.getAllPartitions();
        assertEquals(3, partitionsList.size());

        return s3StateStore;
    }

    private void setupPartitionsAndAddFileInfo(StateStore stateStore) throws IOException, StateStoreException {
        //  - Get root partition
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
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
        Partition leftPartition = new Partition();
        leftPartition.setLeafPartition(true);
        Range leftRange = new RangeFactory(KEY_VALUE_SCHEMA).createRange(KEY_VALUE_SCHEMA.getRowKeyFields().get(0), "0", "eee");
        Region leftRegion = new Region(leftRange);
        leftPartition.setRegion(leftRegion);
        leftPartition.setId("0" + "---eee");
        leftPartition.setParentPartitionId(rootPartition.getId());
        leftPartition.setChildPartitionIds(new ArrayList<>());
        Partition rightPartition = new Partition();
        rightPartition.setLeafPartition(true);
        Range rightRange = new RangeFactory(KEY_VALUE_SCHEMA).createRange(KEY_VALUE_SCHEMA.getRowKeyFields().get(0), "eee", "zzz");
        Region rightRegion = new Region(rightRange);
        rightPartition.setRegion(rightRegion);
        rightPartition.setId("eee---zzz");
        rightPartition.setParentPartitionId(rootPartition.getId());
        rightPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);

        //  - Update Dynamo state store with details of files
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2, fileInfo3));
    }

    private FileInfo createFileInfo(String filename, FileInfo.FileStatus fileStatus, String partitionId,
                                    Key minRowKey, Key maxRowKey) {
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(new StringType());
        fileInfo.setFilename(filename);
        fileInfo.setFileStatus(fileStatus);
        fileInfo.setPartitionId(partitionId);
        fileInfo.setNumberOfRecords(100L);
        fileInfo.setMinRowKey(minRowKey);
        fileInfo.setMaxRowKey(maxRowKey);
        fileInfo.setLastStateStoreUpdateTime(100L);

        return  fileInfo;
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
        instanceProperties.set(TABLE_PROPERTIES, TABLES_PREFIX + "/" + tableName);
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
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, "sleeper" + "-" + tableName + "-" + "active-files" );
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, "sleeper" + "-" + tableName + "-" + "gc-files" );
        tableProperties.set(PARTITION_TABLENAME, "sleeper" + "-" + tableName + "-" + "partitions");
        tableProperties.set(REVISION_TABLENAME, "sleeper" + "-" + tableName + "-" + "revisions" );
        if (isS3StateStore) {
            tableProperties.set(TableProperty.STATESTORE_CLASSNAME, S3_STATE_STORE_CLASS);
            tableProperties.set(DATA_BUCKET, folder.newFolder().getAbsolutePath());
        } else {
            tableProperties.set(TableProperty.STATESTORE_CLASSNAME, DYNAMO_STATE_STORE_CLASS);
        }
        return tableProperties;
    }

    private String createSplitPointsFile(boolean encoded) throws IOException {
        String folderName = folder.newFolder().getAbsolutePath();
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
}
