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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TablePropertiesStore;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableLister;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class GarbageCollectorIT {
    private static final Schema TEST_SCHEMA = getSchema();
    private static final String TEST_TABLE_NAME = "test-table";
    private static final String TEST_TABLE_NAME_1 = "test-table-1";
    private static final String TEST_TABLE_NAME_2 = "test-table-2";
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

    @TempDir
    public java.nio.file.Path tempDir;
    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDBClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());

    @AfterEach
    void tearDown() {
        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    @Nested
    @DisplayName("Collecting from single table")
    class SingleTable {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;
        private StateStoreProvider stateStoreProvider;

        StateStore setupStateStoreAndFixTime(Instant fixedTime) throws Exception {
            new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
            stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, null);
            DynamoDBStateStore stateStore = (DynamoDBStateStore) stateStoreProvider.getStateStore(tableProperties);
            stateStore.initialise();
            stateStore.fixTime(fixedTime);
            return stateStore;
        }

        @Test
        void shouldCollectFileMarkedAsReadyForGCAfterSpecifiedDelay() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(timeAfterDelay);
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createReadyForGCFile(filePath.toString(), stateStore);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(filePath)).isFalse();
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties)).isEmpty();
        }

        @Test
        void shouldNotCollectFileMarkedAsActive() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTable(TEST_TABLE_NAME, instanceProperties);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            StateStore stateStore = setupStateStoreAndFixTime(currentTime);
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createActiveFile(filePath.toString(), stateStore);

            // When
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(filePath)).isTrue();
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties)).isEmpty();
        }

        @Test
        void shouldNotCollectFileMarkedAsReadyForGCBeforeSpecifiedDelay() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTable(TEST_TABLE_NAME, instanceProperties);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            StateStore stateStore = setupStateStoreAndFixTime(currentTime);
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createReadyForGCFile(filePath.toString(), stateStore);

            // When
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(filePath)).isTrue();
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties))
                    .extracting(FileInfo::getFilename)
                    .containsExactly(filePath.toString());
        }

        @Test
        void shouldCollectMultipleFilesInOneRun() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(timeAfterDelay);
            java.nio.file.Path filePath1 = tempDir.resolve("test-file-1.parquet");
            java.nio.file.Path filePath2 = tempDir.resolve("test-file-2.parquet");
            createReadyForGCFile(filePath1.toString(), stateStoreProvider.getStateStore(tableProperties));
            createReadyForGCFile(filePath2.toString(), stateStoreProvider.getStateStore(tableProperties));

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(filePath1)).isFalse();
            assertThat(Files.exists(filePath2)).isFalse();
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties)).isEmpty();
        }

        @Test
        void shouldNotCollectMoreFilesIfBatchSizeExceeded() throws Exception {
            // Given
            instanceProperties = createInstancePropertiesWithGCBatchSize(2);
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(timeAfterDelay);
            java.nio.file.Path filePath1 = tempDir.resolve("test-file-1.parquet");
            java.nio.file.Path filePath2 = tempDir.resolve("test-file-2.parquet");
            java.nio.file.Path filePath3 = tempDir.resolve("test-file-3.parquet");
            createReadyForGCFile(filePath1.toString(), stateStoreProvider.getStateStore(tableProperties));
            createReadyForGCFile(filePath2.toString(), stateStoreProvider.getStateStore(tableProperties));
            createReadyForGCFile(filePath3.toString(), stateStoreProvider.getStateStore(tableProperties));

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Stream.of(filePath1, filePath2, filePath3).filter(Files::exists))
                    .hasSize(1);
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties))
                    .hasSize(1);
        }
    }

    @Nested
    @DisplayName("Collecting from multiple tables")
    class MultipleTables {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties1;
        private TableProperties tableProperties2;
        private StateStoreProvider stateStoreProvider;

        void setupStateStores() throws Exception {
            new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
            stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, null);
            DynamoDBStateStore stateStore1 = (DynamoDBStateStore) stateStoreProvider.getStateStore(tableProperties1);
            stateStore1.initialise();
            DynamoDBStateStore stateStore2 = (DynamoDBStateStore) stateStoreProvider.getStateStore(tableProperties2);
            stateStore2.initialise();
        }

        @Test
        void shouldCollectOneFileFromEachTable() throws Exception {
            // Given
            instanceProperties = createInstancePropertiesWithGCBatchSize(2);
            tableProperties1 = createTableWithGCDelay(TEST_TABLE_NAME_1, instanceProperties, 10);
            tableProperties2 = createTableWithGCDelay(TEST_TABLE_NAME_2, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            setupStateStores();
            StateStore stateStore1 = stateStoreProvider.getStateStore(tableProperties1);
            StateStore stateStore2 = stateStoreProvider.getStateStore(tableProperties2);
            java.nio.file.Path filePath1 = tempDir.resolve("test-file-1.parquet");
            java.nio.file.Path filePath2 = tempDir.resolve("test-file-2.parquet");
            stateStore1.fixTime(timeAfterDelay);
            stateStore2.fixTime(timeAfterDelay);
            createReadyForGCFile(filePath1.toString(), stateStore1);
            createReadyForGCFile(filePath2.toString(), stateStore2);

            // When
            stateStore1.fixTime(currentTime);
            stateStore2.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(filePath1)).isFalse();
            assertThat(Files.exists(filePath2)).isFalse();
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties1)).isEmpty();
            assertThat(getFilesInReadyForGCTable(instanceProperties, tableProperties2)).isEmpty();
        }
    }

    private Iterable<FileInfo> getFilesInReadyForGCTable(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0");
        StateStore stateStore = new StateStoreFactory(dynamoDBClient, instanceProperties, new Configuration())
                .getStateStore(tableProperties);
        return () -> {
            try {
                return stateStore.getReadyForGCFiles();
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void createActiveFile(String filename, StateStore stateStore) throws Exception {
        createFile(filename, stateStore, FileInfo.FileStatus.ACTIVE);
    }

    private void createReadyForGCFile(String filename, StateStore stateStore) throws Exception {
        createFile(filename, stateStore, FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
    }

    private void createFile(String filename, StateStore stateStore, FileInfo.FileStatus status) throws Exception {
        String partitionId = stateStore.getAllPartitions().get(0).getId();
        FileInfo fileInfo = FileInfo.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(100L)
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

    private InstanceProperties createInstancePropertiesWithGCBatchSize(int gcBatchSize) {
        return createInstanceProperties(properties ->
                properties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, gcBatchSize));
    }

    private InstanceProperties createInstanceProperties() {
        return createInstanceProperties(properties -> {
        });
    }

    private InstanceProperties createInstanceProperties(Consumer<InstanceProperties> extraProperties) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);

        extraProperties.accept(instanceProperties);
        return instanceProperties;
    }

    private TableProperties createTableWithGCDelay(String tableName, InstanceProperties instanceProperties, int gcDelay) {
        return createTable(tableName, instanceProperties, tableProperties ->
                tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, gcDelay));
    }

    private TableProperties createTable(String tableName, InstanceProperties instanceProperties) {
        return createTable(tableName, instanceProperties, tableProperties -> {
        });
    }

    private TableProperties createTable(
            String tableName, InstanceProperties instanceProperties, Consumer<TableProperties> extraProperties) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
        tableProperties.set(TABLE_NAME, tableName);
        extraProperties.accept(tableProperties);
        new S3TablePropertiesStore(instanceProperties, s3Client, dynamoDBClient).save(tableProperties);
        return tableProperties;
    }

    private GarbageCollector createGarbageCollector(InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider) {
        return new GarbageCollector(new Configuration(), new TableLister(s3Client, instanceProperties),
                new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient), stateStoreProvider,
                instanceProperties.getInt(GARBAGE_COLLECTOR_BATCH_SIZE));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
