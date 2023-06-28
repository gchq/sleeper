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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
    private static final String TEST_TABLE_NAME_1 = "test-table-1";
    private static final String TEST_TABLE_NAME_2 = "test-table-2";
    private static final int DEFAULT_BATCH_SIZE = 2;
    private static final int DEFAULT_GC_DELAY = 10;
    private static final Instant CURRENT_TIME = Instant.parse("2023-06-28T13:46:00Z");
    private static final Instant TIME_EXCEEDING_DELAY = CURRENT_TIME.minus(Duration.ofMinutes(DEFAULT_GC_DELAY + 1));
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
        dynamoDBClient.shutdown();
    }

    @Nested
    @DisplayName("Collecting from single table")
    class SingleTable {
        private TableProperties tableProperties;
        private DynamoDBStateStore stateStore;
        private GarbageCollector garbageCollector;

        @BeforeEach
        void setup() throws Exception {
            // Given
            InstanceProperties instanceProperties = createInstanceProperties();
            tableProperties = createTable(TEST_TABLE_NAME, instanceProperties);
            createDynamoDBStateStore(instanceProperties, tableProperties);
            StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
            stateStore = (DynamoDBStateStore) stateStoreProvider.getStateStore(tableProperties);
            stateStore.initialise();
            stateStore.fixTime(CURRENT_TIME);
            garbageCollector = createGarbageCollector(s3Client, instanceProperties, stateStoreProvider);
        }

        @AfterEach
        void tearDown() {
            dynamoDBClient.deleteTable(TEST_TABLE_NAME + "-af");
            dynamoDBClient.deleteTable(TEST_TABLE_NAME + "-rfgcf");
            dynamoDBClient.deleteTable(TEST_TABLE_NAME + "-p");
        }

        @Test
        void shouldCollectFileMarkedAsReadyForGCAfterSpecifiedDelay() throws Exception {
            // Given
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createReadyForGCFile(filePath.toString(), stateStore, TIME_EXCEEDING_DELAY.toEpochMilli());

            // When
            garbageCollector.run();

            // Then
            assertThat(Files.exists(filePath)).isFalse();
            assertThat(getFilesInReadyForGCTable(tableProperties)).isEmpty();
        }

        @Test
        void shouldNotCollectFileMarkedAsActive() throws Exception {
            // Given
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createActiveFile(filePath.toString(), stateStore);

            // When
            garbageCollector.run();

            // Then
            assertThat(Files.exists(filePath)).isTrue();
            assertThat(getFilesInReadyForGCTable(tableProperties)).isEmpty();
        }

        @Test
        void shouldNotCollectFileMarkedAsReadyForGCBeforeSpecifiedDelay() throws Exception {
            // Given
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createReadyForGCFile(filePath.toString(), stateStore, CURRENT_TIME.toEpochMilli());

            // When
            garbageCollector.run();

            // Then
            assertThat(Files.exists(filePath)).isTrue();
            assertThat(getFilesInReadyForGCTable(tableProperties))
                    .containsExactly(filePath.toString());
        }

        @Test
        void shouldCollectMultipleFilesInOneRun() throws Exception {
            // Given
            java.nio.file.Path filePath1 = tempDir.resolve("test-file-1.parquet");
            java.nio.file.Path filePath2 = tempDir.resolve("test-file-2.parquet");
            createReadyForGCFile(filePath1.toString(), stateStore, TIME_EXCEEDING_DELAY.toEpochMilli());
            createReadyForGCFile(filePath2.toString(), stateStore, TIME_EXCEEDING_DELAY.toEpochMilli());

            // When
            garbageCollector.run();

            // Then
            assertThat(Files.exists(filePath1)).isFalse();
            assertThat(Files.exists(filePath2)).isFalse();
            assertThat(getFilesInReadyForGCTable(tableProperties)).isEmpty();
        }

        @Test
        void shouldNotCollectMoreFilesIfBatchSizeExceeded() throws Exception {
            // Given
            java.nio.file.Path filePath1 = tempDir.resolve("test-file-1.parquet");
            java.nio.file.Path filePath2 = tempDir.resolve("test-file-2.parquet");
            java.nio.file.Path filePath3 = tempDir.resolve("test-file-3.parquet");
            createReadyForGCFile(filePath1.toString(), stateStore, TIME_EXCEEDING_DELAY.toEpochMilli());
            createReadyForGCFile(filePath2.toString(), stateStore, TIME_EXCEEDING_DELAY.toEpochMilli());
            createReadyForGCFile(filePath3.toString(), stateStore, TIME_EXCEEDING_DELAY.toEpochMilli());

            // When
            garbageCollector.run();

            // Then
            assertThat(Stream.of(filePath1, filePath2, filePath3).filter(Files::exists))
                    .hasSize(1);
            assertThat(getFilesInReadyForGCTable(tableProperties))
                    .hasSize(1);
        }
    }

    @Nested
    @DisplayName("Collecting from multiple tables")
    class MultipleTables {
        private TableProperties tableProperties1;
        private DynamoDBStateStore stateStore1;
        private TableProperties tableProperties2;
        private DynamoDBStateStore stateStore2;
        private GarbageCollector garbageCollector;

        @BeforeEach
        void setup() throws Exception {
            InstanceProperties instanceProperties = createInstanceProperties();
            tableProperties1 = createTable(TEST_TABLE_NAME_1, instanceProperties);
            tableProperties2 = createTable(TEST_TABLE_NAME_2, instanceProperties);
            createDynamoDBStateStore(instanceProperties, tableProperties1);
            createDynamoDBStateStore(instanceProperties, tableProperties2);
            StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
            stateStore1 = (DynamoDBStateStore) stateStoreProvider.getStateStore(tableProperties1);
            stateStore1.initialise();
            stateStore1.fixTime(CURRENT_TIME);
            stateStore2 = (DynamoDBStateStore) stateStoreProvider.getStateStore(tableProperties2);
            stateStore2.initialise();
            stateStore2.fixTime(CURRENT_TIME);
            garbageCollector = createGarbageCollector(s3Client, instanceProperties, stateStoreProvider);
        }

        @AfterEach
        void tearDown() {
            List.of(TEST_TABLE_NAME_1, TEST_TABLE_NAME_2).forEach(table -> {
                dynamoDBClient.deleteTable(table + "-af");
                dynamoDBClient.deleteTable(table + "-rfgcf");
                dynamoDBClient.deleteTable(table + "-p");
            });
        }

        @Test
        void shouldCollectOneFileFromEachTable() throws Exception {
            // Given
            java.nio.file.Path filePath1 = tempDir.resolve("test-file-1.parquet");
            java.nio.file.Path filePath2 = tempDir.resolve("test-file-2.parquet");
            createReadyForGCFile(filePath1.toString(), stateStore1, TIME_EXCEEDING_DELAY.toEpochMilli());
            createReadyForGCFile(filePath2.toString(), stateStore2, TIME_EXCEEDING_DELAY.toEpochMilli());

            // When
            garbageCollector.run();

            // Then
            assertThat(Files.exists(filePath1)).isFalse();
            assertThat(Files.exists(filePath2)).isFalse();
            assertThat(getFilesInReadyForGCTable(tableProperties1)).isEmpty();
            assertThat(getFilesInReadyForGCTable(tableProperties2)).isEmpty();
        }
    }

    private Stream<String> getFilesInReadyForGCTable(TableProperties tableProperties) {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        return scanResult.getItems().stream().map(item -> item.get(DynamoDBStateStore.FILE_NAME).getS());
    }

    private void createActiveFile(String filename, StateStore stateStore) throws Exception {
        createFile(filename, stateStore, FileInfo.FileStatus.ACTIVE, CURRENT_TIME.toEpochMilli());
    }

    private void createReadyForGCFile(String filename, StateStore stateStore, long lastUpdateTime) throws Exception {
        createFile(filename, stateStore, FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION, lastUpdateTime);
    }

    private void createFile(String filename, StateStore stateStore, FileInfo.FileStatus status, long lastUpdateTime) throws Exception {
        String partitionId = stateStore.getAllPartitions().get(0).getId();
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename(filename)
                .partitionId(partitionId)
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(lastUpdateTime)
                .fileStatus(status)
                .build();
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), TEST_SCHEMA);
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

    private TableProperties createTable(String tableName, InstanceProperties instanceProperties) throws IOException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(TEST_SCHEMA);
        tableProperties.set(DATA_BUCKET, tempDir.toString());
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "" + DEFAULT_GC_DELAY);
        tableProperties.saveToS3(s3Client);
        return tableProperties;
    }

    private static GarbageCollector createGarbageCollector(AmazonS3 s3Client, InstanceProperties instanceProperties,
                                                           StateStoreProvider stateStoreProvider) {
        return new GarbageCollector(new Configuration(), new TableLister(s3Client, instanceProperties),
                new TablePropertiesProvider(s3Client, instanceProperties), stateStoreProvider, DEFAULT_BATCH_SIZE);
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
