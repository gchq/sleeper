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
package sleeper.ingest.impl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.ingest.testutils.TestIngestType;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.ingest.testutils.TestIngestType.directWriteBackedByArrowWriteToLocalFile;

@Testcontainers
class IngestCoordinatorUsingDirectWriteBackedByArrowIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB,
            AmazonDynamoDBClientBuilder.standard());
    private final Configuration configuration = AwsExternalResource.getHadoopConfiguration(localStackContainer);
    private static final String TABLE_NAME = "test-table";
    private static final String DATA_BUCKET_NAME = "databucket";
    @TempDir
    public Path temporaryFolder;

    @AfterEach
    public void after() {
        dynamoDB.deleteTable(TABLE_NAME + "-af");
        dynamoDB.deleteTable(TABLE_NAME + "-rgcf");
        dynamoDB.deleteTable(TABLE_NAME + "-p");
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(
                TABLE_NAME, recordListAndSchema.sleeperSchema, dynamoDB).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(() -> stateStoreUpdateTime)
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig = config -> config
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(128 * 1024 * 1024L);
        TestIngestType ingestType = directWriteBackedByArrowWriteToLocalFile(arrowConfig);
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo leftFile = fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) +
                "/partition_left/leftFile.parquet", 10000, -10000L, -1L);
        FileInfo rightFile = fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) +
                "/partition_right/rightFile.parquet", 10000, 0L, 9999L);

        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, actualFiles, configuration);

        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyInAnyOrderElementsOf(LongStream.range(-10000, 10000).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                AwsExternalResource.getHadoopConfiguration(localStackContainer)
        );
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(
                TABLE_NAME, recordListAndSchema.sleeperSchema, dynamoDB).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                .fileUpdatedTimes(() -> stateStoreUpdateTime)
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When
        Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig = arrow -> arrow
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(4 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(16 * 1024 * 1024L);
        TestIngestType ingestType = directWriteBackedByArrowWriteToLocalFile(arrowConfig);
        ingestRecords(recordListAndSchema, parameters, ingestType);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo leftFile1 = fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) +
                "/partition_left/leftFile1.parquet", 7908, -10000L, -2L);
        FileInfo leftFile2 = fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) +
                "/partition_left/leftFile2.parquet", 2092, -9998L, -1L);
        FileInfo rightFile1 = fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) +
                "/partition_right/rightFile1.parquet", 7791, 1L, 9998L);
        FileInfo rightFile2 = fileInfoFactory.leafFile(ingestType.getFilePrefix(parameters) +
                "/partition_right/rightFile2.parquet", 2209, 0L, 9999L);

        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(
                recordListAndSchema.sleeperSchema, actualFiles, configuration);

        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile1, leftFile2, rightFile1, rightFile2);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyInAnyOrderElementsOf(LongStream.range(-10000, 10000).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                AwsExternalResource.getHadoopConfiguration(localStackContainer)
        );
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(
                TABLE_NAME, recordListAndSchema.sleeperSchema, dynamoDB).create();
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        stateStore.initialise(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .fileUpdatedTimes(() -> stateStoreUpdateTime)
                .stateStore(stateStore)
                .schema(recordListAndSchema.sleeperSchema)
                .workingDir(ingestLocalWorkingDirectory)
                .build();

        // When/Then
        Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig = arrow -> arrow
                .workingBufferAllocatorBytes(32 * 1024L)
                .batchBufferAllocatorBytes(1024 * 1024L)
                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L);
        TestIngestType ingestType = directWriteBackedByArrowWriteToLocalFile(arrowConfig);
        assertThatThrownBy(() -> ingestRecords(recordListAndSchema, parameters, ingestType))
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private IngestCoordinatorTestParameters.Builder createTestParameterBuilder() {
        return IngestCoordinatorTestParameters
                .builder()
                .temporaryFolder(temporaryFolder)
                .hadoopConfiguration(AwsExternalResource.getHadoopConfiguration(localStackContainer))
                .dataBucketName(DATA_BUCKET_NAME);
    }

    private static void ingestRecords(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                      IngestCoordinatorTestParameters ingestCoordinatorTestParameters,
                                      TestIngestType ingestType) throws Exception {
        try (IngestCoordinator<Record> ingestCoordinator =
                     ingestType.createIngestCoordinator(
                             ingestCoordinatorTestParameters)) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }
}
