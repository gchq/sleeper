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
package sleeper.statestore.transactionlog.snapshots;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.parquet.utils.HadoopConfigurationLocalStackUtils;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotStore.LatestSnapshotsMetadataLoader;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotStore.SnapshotMetadataSaver;
import sleeper.statestore.transactionlog.snapshots.TransactionLogSnapshotDeleter.DeleteProcesState;
import sleeper.statestore.transactionlog.snapshots.TransactionLogSnapshotDeleter.SnapshotFileDeleter;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotStore.getBasePath;

@Testcontainers
public class TransactionLogSnapshotTestBase {
    @TempDir
    private java.nio.file.Path tempDir;
    private FileSystem fs;
    protected final Schema schema = schemaWithKey("key", new LongType());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    protected static AmazonDynamoDB dynamoDBClient;
    protected static AmazonS3 s3Client;
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Configuration configuration = HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer);

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    @BeforeEach
    public void setup() throws IOException {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();
        fs = FileSystem.get(configuration);
    }

    protected TransactionLogSnapshotMetadata getLatestPartitionsSnapshot(TableProperties table) {
        return snapshotStore(table).getLatestSnapshots().getPartitionsSnapshot().orElseThrow();
    }

    protected TransactionLogSnapshotMetadata getLatestFilesSnapshot(TableProperties table) {
        return snapshotStore(table).getLatestSnapshots().getFilesSnapshot().orElseThrow();
    }

    protected void deleteSnapshotFile(TransactionLogSnapshotMetadata snapshot) throws Exception {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(snapshot.getPath());
        FileSystem fs = path.getFileSystem(configuration);
        fs.delete(path, false);
    }

    protected void createSnapshots(TableProperties table) {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(table);
        createSnapshots(table, snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    protected void createSnapshots(TableProperties table, LatestSnapshotsMetadataLoader latestSnapshotsLoader) {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(table);
        createSnapshots(table, latestSnapshotsLoader, snapshotStore::saveSnapshot);
    }

    protected void createSnapshots(
            TableProperties table, SnapshotMetadataSaver snapshotSaver) {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(table);
        createSnapshots(table, snapshotStore::getLatestSnapshots, snapshotSaver);
    }

    protected void createSnapshotsAt(TableProperties table, Instant creationTime) throws Exception {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, table, dynamoDBClient, () -> creationTime);
        createSnapshots(table, snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    protected void createSnapshots(
            TableProperties table, LatestSnapshotsMetadataLoader latestSnapshotsLoader, SnapshotMetadataSaver snapshotSaver) {
        new DynamoDBTransactionLogSnapshotCreator(
                instanceProperties, table,
                transactionLogs.forTable(table).getFilesLogStore(),
                transactionLogs.forTable(table).getPartitionsLogStore(),
                configuration, latestSnapshotsLoader, snapshotSaver)
                .createSnapshot();
    }

    protected DeleteProcesState deleteSnapshotsAt(TableProperties table, Instant deletionTime) {
        return new TransactionLogSnapshotDeleter(
                instanceProperties, table, dynamoDBClient, configuration)
                .deleteSnapshots(deletionTime);
    }

    protected DeleteProcesState deleteSnapshotsAt(TableProperties table, Instant deletionTime, SnapshotFileDeleter fileDeleter) {
        return new TransactionLogSnapshotDeleter(
                instanceProperties, table, dynamoDBClient, fileDeleter)
                .deleteSnapshots(deletionTime);
    }

    protected StateStore createStateStoreWithInMemoryTransactionLog(TableProperties table) {
        StateStore stateStore = transactionLogs.stateStoreBuilder(table).build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    protected DynamoDBTransactionLogSnapshotMetadataStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, table, dynamoDBClient);
    }

    protected TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
        return tableProperties;
    }

    protected TransactionLogSnapshotMetadata filesSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(getBasePath(instanceProperties, table), transactionNumber);
    }

    protected TransactionLogSnapshotMetadata partitionsSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forPartitions(getBasePath(instanceProperties, table), transactionNumber);
    }

    protected String filesSnapshotPath(TableProperties table, long transactionNumber) {
        return filesSnapshot(table, transactionNumber).getPath();
    }

    protected String partitionsSnapshotPath(TableProperties table, long transactionNumber) {
        return partitionsSnapshot(table, transactionNumber).getPath();
    }

    protected boolean filesSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        return fs.exists(new Path(filesSnapshot(table, transactionNumber).getPath()));
    }

    protected boolean partitionsSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        return fs.exists(new Path(partitionsSnapshot(table, transactionNumber).getPath()));
    }

    protected void deleteFilesSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        fs.delete(new Path(filesSnapshot(table, transactionNumber).getPath()), false);
    }

    protected void deletePartitionsSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        fs.delete(new org.apache.hadoop.fs.Path(partitionsSnapshot(table, transactionNumber).getPath()), false);
    }

    protected Stream<String> tableFiles(TableProperties tableProperties) throws Exception {
        Path tableFilesPath = new Path(getBasePath(instanceProperties, tableProperties));
        if (!fs.exists(tableFilesPath)) {
            return Stream.empty();
        }
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(tableFilesPath, true);
        List<String> files = new ArrayList<>();
        while (iterator.hasNext()) {
            files.add(iterator.next().getPath().toUri().toString());
        }
        return files.stream();
    }
}
