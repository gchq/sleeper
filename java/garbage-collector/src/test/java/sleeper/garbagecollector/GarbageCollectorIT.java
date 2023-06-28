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
package sleeper.garbagecollector;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableLister;

import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@Testcontainers
public class GarbageCollectorIT {
    private static final Schema TEST_SCHEMA = getSchema();
    private static final String TEST_TABLE_NAME = "test-table";
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

    @TempDir
    public java.nio.file.Path tempDir;
    private final AmazonS3 s3Client = createS3Client();
    private final AmazonDynamoDB dynamoDBClient = createDynamoClient();

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .build();
    }

    @AfterEach
    void tearDown() {
        s3Client.shutdown();
        dynamoDBClient.deleteTable(TEST_TABLE_NAME + "-af");
        dynamoDBClient.deleteTable(TEST_TABLE_NAME + "-rfgcf");
        dynamoDBClient.deleteTable(TEST_TABLE_NAME + "-p");
        dynamoDBClient.shutdown();
    }

    @Test
    void shouldCollectFileMarkedAsReadyForGCAfterSpecifiedDelay() throws Exception {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties);
        createDynamoDBStateStore(instanceProperties, tableProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        GarbageCollector garbageCollector = createGarbageCollector(s3Client, instanceProperties, stateStoreProvider);

        // When
        createReadyForGCFile("test-file.parquet", stateStore, System.currentTimeMillis() - 20L * 60L * 1000L);
        garbageCollector.run();

        // Then
        assertThat(Files.exists(tempDir.resolve("test-file.parquet"))).isFalse();
        assertThat(getFilesInReadyForGCTable(tableProperties)).isEmpty();
    }

    @Test
    void shouldNotCollectFileMarkedAsActive() throws Exception {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties);
        createDynamoDBStateStore(instanceProperties, tableProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        GarbageCollector garbageCollector = createGarbageCollector(s3Client, instanceProperties, stateStoreProvider);

        // When
        createActiveFile("test-file.parquet", stateStore);
        garbageCollector.run();

        // Then
        assertThat(Files.exists(tempDir.resolve("test-file.parquet"))).isTrue();
        assertThat(getFilesInReadyForGCTable(tableProperties)).isEmpty();
    }

    @Test
    void shouldNotCollectFileMarkedAsReadyForGCBeforeSpecifiedDelay() throws Exception {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties);
        createDynamoDBStateStore(instanceProperties, tableProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        GarbageCollector garbageCollector = createGarbageCollector(s3Client, instanceProperties, stateStoreProvider);

        // When
        createReadyForGCFile("test-file.parquet", stateStore, System.currentTimeMillis());
        garbageCollector.run();

        // Then
        assertThat(Files.exists(tempDir.resolve("test-file.parquet"))).isTrue();
        assertThat(getFilesInReadyForGCTable(tableProperties))
                .containsExactly(tempDir.resolve("test-file.parquet").toString());
    }

    private Stream<String> getFilesInReadyForGCTable(TableProperties tableProperties) {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        return scanResult.getItems().stream().map(item -> item.get(DynamoDBStateStore.FILE_NAME).getS());
    }

    private void createActiveFile(String filename, StateStore stateStore) throws Exception {
        createFile(filename, stateStore, FileInfo.FileStatus.ACTIVE, System.currentTimeMillis());
    }

    private void createReadyForGCFile(String filename, StateStore stateStore, long lastUpdateTime) throws Exception {
        createFile(filename, stateStore, FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION, lastUpdateTime);
    }

    private void createFile(String filename, StateStore stateStore, FileInfo.FileStatus status, long lastUpdateTime) throws Exception {
        String partitionId = stateStore.getAllPartitions().get(0).getId();
        String filePath = tempDir.resolve(filename).toString();
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename(filePath)
                .partitionId(partitionId)
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(lastUpdateTime)
                .fileStatus(status)
                .build();
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filePath), TEST_SCHEMA);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer.write(record);
        }
        writer.close();
        stateStore.addFile(fileInfo);
    }

    private void createDynamoDBStateStore(InstanceProperties instanceProperties, TableProperties tableProperties)
            throws StateStoreException {
        new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDBClient).create();
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, "");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));

        return instanceProperties;
    }

    private TableProperties createTable(InstanceProperties instanceProperties) throws IOException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, TEST_TABLE_NAME);
        tableProperties.setSchema(TEST_SCHEMA);
        tableProperties.set(DATA_BUCKET, tempDir.toString());
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, TEST_TABLE_NAME + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, TEST_TABLE_NAME + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, TEST_TABLE_NAME + "-p");
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "10");
        tableProperties.saveToS3(s3Client);
        return tableProperties;
    }

    private static GarbageCollector createGarbageCollector(AmazonS3 s3Client, InstanceProperties instanceProperties,
                                                           StateStoreProvider stateStoreProvider) {
        return new GarbageCollector(new Configuration(), new TableLister(s3Client, instanceProperties),
                new TablePropertiesProvider(s3Client, instanceProperties), stateStoreProvider, 10);
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
