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
import sleeper.configuration.properties.table.S3TableProperties;
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
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.FileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

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
            new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
            stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, getHadoopConfiguration(localStackContainer));
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            stateStore.initialise();
            stateStore.fixTime(fixedTime);
            return stateStore;
        }

        @Test
        void shouldCollectFileWithNoReferencesAfterSpecifiedDelay() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(timeAfterDelay);
            java.nio.file.Path oldFile = tempDir.resolve("old-file.parquet");
            java.nio.file.Path newFile = tempDir.resolve("new-file.parquet");
            createFileWithNoReferencesByCompaction(stateStore, timeAfterDelay, oldFile, newFile);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(oldFile)).isFalse();
            assertThat(stateStore.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .containsExactly(newFile.toString());
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
            assertThat(stateStore.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .containsExactly(filePath.toString());
        }

        @Test
        void shouldNotCollectFileWithNoReferencesBeforeSpecifiedDelay() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTable(TEST_TABLE_NAME, instanceProperties);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            StateStore stateStore = setupStateStoreAndFixTime(currentTime);
            java.nio.file.Path oldFile = tempDir.resolve("old-file.parquet");
            java.nio.file.Path newFile = tempDir.resolve("new-file.parquet");
            createFileWithNoReferencesByCompaction(stateStore, currentTime, oldFile, newFile);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(oldFile)).isFalse();
            assertThat(stateStore.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .containsExactly(newFile.toString());
        }

        @Test
        void shouldCollectMultipleFilesInOneRun() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(timeAfterDelay);
            java.nio.file.Path oldFile1 = tempDir.resolve("old-file-1.parquet");
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile1 = tempDir.resolve("new-file-1.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore, timeAfterDelay, oldFile1, newFile1);
            createFileWithNoReferencesByCompaction(stateStore, timeAfterDelay, oldFile2, newFile2);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(oldFile1)).isFalse();
            assertThat(Files.exists(oldFile2)).isFalse();
            assertThat(stateStore.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .containsExactlyInAnyOrder(newFile1.toString(), newFile2.toString());
        }

        @Test
        void shouldNotCollectMoreFilesIfBatchSizeExceeded() throws Exception {
            // Given
            instanceProperties = createInstancePropertiesWithGCBatchSize(1);
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(timeAfterDelay);
            java.nio.file.Path oldFile1 = tempDir.resolve("old-file-1.parquet");
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile1 = tempDir.resolve("new-file-1.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore, timeAfterDelay, oldFile1, newFile1);
            createFileWithNoReferencesByCompaction(stateStore, timeAfterDelay, oldFile2, newFile2);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Stream.of(oldFile1, oldFile2).filter(Files::exists))
                    .hasSize(1);
            assertThat(stateStore.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .contains(newFile1.toString(), newFile2.toString())
                    .hasSize(3);
        }
    }

    @Nested
    @DisplayName("Collecting from multiple tables")
    class MultipleTables {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties1;
        private TableProperties tableProperties2;
        private StateStoreProvider stateStoreProvider;

        void setupStateStoresAndFixTimes(Instant fixedTime) throws Exception {
            new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
            stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, getHadoopConfiguration(localStackContainer));
            StateStore stateStore1 = stateStoreProvider.getStateStore(tableProperties1);
            stateStore1.initialise();
            stateStore1.fixTime(fixedTime);
            StateStore stateStore2 = stateStoreProvider.getStateStore(tableProperties2);
            stateStore2.initialise();
            stateStore2.fixTime(fixedTime);
        }

        @Test
        void shouldCollectOneFileFromEachTable() throws Exception {
            // Given
            instanceProperties = createInstancePropertiesWithGCBatchSize(2);
            tableProperties1 = createTableWithGCDelay(TEST_TABLE_NAME_1, instanceProperties, 10);
            tableProperties2 = createTableWithGCDelay(TEST_TABLE_NAME_2, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant timeAfterDelay = currentTime.minus(Duration.ofMinutes(11));
            setupStateStoresAndFixTimes(timeAfterDelay);
            StateStore stateStore1 = stateStoreProvider.getStateStore(tableProperties1);
            StateStore stateStore2 = stateStoreProvider.getStateStore(tableProperties2);
            java.nio.file.Path oldFile1 = tempDir.resolve("old-file-1.parquet");
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile1 = tempDir.resolve("new-file-1.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore1, timeAfterDelay, oldFile1, newFile1);
            createFileWithNoReferencesByCompaction(stateStore2, timeAfterDelay, oldFile2, newFile2);

            // When
            stateStore1.fixTime(currentTime);
            stateStore2.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).run();

            // Then
            assertThat(Files.exists(oldFile1)).isFalse();
            assertThat(Files.exists(oldFile2)).isFalse();
            assertThat(stateStore1.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .contains(newFile1.toString());
            assertThat(stateStore2.getAllFileReferences().getFiles())
                    .flatMap(FileReferences::getFilename)
                    .contains(newFile2.toString());
        }
    }

    private void createActiveFile(String filename, StateStore stateStore) throws Exception {
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(TEST_SCHEMA, stateStore);
        FileInfo fileInfo = fileInfoFactory.rootFile(filename, 100L);
        writeFile(filename);
        stateStore.addFile(fileInfo);
    }

    private void createFileWithNoReferencesByCompaction(StateStore stateStore, Instant timeAfterDelay,
                                                        java.nio.file.Path oldFilePath, java.nio.file.Path newFilePath) throws Exception {
        FileInfoFactory factory = FileInfoFactory.fromUpdatedAt(TEST_SCHEMA, stateStore, timeAfterDelay);
        FileInfo oldFile = factory.rootFile(oldFilePath.toString(), 100);
        writeFile(oldFilePath.toString());
        stateStore.addFile(oldFile);
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile("root", List.of(oldFile.getFilename()),
                factory.rootFile(newFilePath.toString(), 100));
        writeFile(newFilePath.toString());
    }

    private void writeFile(String filename) throws Exception {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), TEST_SCHEMA);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer.write(record);
        }
        writer.close();
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
        instanceProperties.set(FILE_SYSTEM, "file://");
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
        S3TableProperties.getStore(instanceProperties, s3Client, dynamoDBClient).save(tableProperties);
        return tableProperties;
    }

    private GarbageCollector createGarbageCollector(InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider) {
        return new GarbageCollector(getHadoopConfiguration(localStackContainer),
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
