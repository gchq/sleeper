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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arraylist.ArrayListRecordBatchFactory;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.AwsExternalResource.getHadoopConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.ingest.testutils.ResultVerifier.readRecordsFromPartitionDataFile;

@Testcontainers
public class IngestCoordinatorUsingDirectWriteBackedByArrayListIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    private final String dataBucketName = UUID.randomUUID().toString();
    @TempDir
    public Path temporaryFolder;
    private final Configuration hadoopConfiguration = getHadoopConfiguration(localStackContainer);
    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @BeforeEach
    public void before() {
        s3.createBucket(dataBucketName);
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDB).create();
    }

    private StateStore createStateStore(Schema schema) {
        tableProperties.setSchema(schema);
        return new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDB);
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));

        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
        stateStore.initialise(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");

        ingestRecords(
                recordListAndSchema,
                stateStore,
                5,
                1000L,
                ingestLocalWorkingDirectory,
                Stream.of("leftFile", "rightFile"),
                stateStoreUpdateTime);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo leftFile = fileInfoFactory.partitionFile("left", "s3a://" + dataBucketName + "/partition_left/leftFile.parquet", 100, -100L, -1L);
        FileInfo rightFile = fileInfoFactory.partitionFile("right", "s3a://" + dataBucketName + "/partition_right/rightFile.parquet", 100, 0L, 99L);
        List<Record> leftRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, leftFile, hadoopConfiguration);
        List<Record> rightRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, rightFile, hadoopConfiguration);
        List<Record> allRecords = Stream.of(leftRecords, rightRecords).flatMap(List::stream).collect(Collectors.toUnmodifiableList());

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrder(leftFile, rightFile);
        assertThat(allRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(leftRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(LongStream.range(-100, 0).boxed().toArray());
        assertThat(rightRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(LongStream.range(0, 100).boxed().toArray());

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalStore() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        PartitionTree tree = new PartitionsBuilder(recordListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildTree();
        Instant stateStoreUpdateTime = Instant.parse("2023-08-08T11:20:00Z");
        StateStore stateStore = createStateStore(recordListAndSchema.sleeperSchema);
        stateStore.initialise(tree.getAllPartitions());
        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString();
        Stream<String> fileNames = IntStream.iterate(0, i -> i + 1).mapToObj(i -> "file" + i);

        // When
        ingestRecords(
                recordListAndSchema,
                stateStore,
                5,
                10L,
                ingestLocalWorkingDirectory,
                fileNames,
                stateStoreUpdateTime);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();
        FileInfo firstLeftFile = fileInfoFactory.partitionFile("left", "s3a://" + dataBucketName + "/partition_left/file0.parquet", 5, -90L, -2L);
        FileInfo firstRightFile = fileInfoFactory.partitionFile("right", "s3a://" + dataBucketName + "/partition_right/file1.parquet", 5, 12L, 83L);
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        List<Record> firstLeftFileRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, firstLeftFile, hadoopConfiguration);
        List<Record> firstRightFileRecords = readRecordsFromPartitionDataFile(recordListAndSchema.sleeperSchema, firstRightFile, hadoopConfiguration);

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles)
                .hasSize(40)
                .contains(firstLeftFile, firstRightFile);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(firstLeftFileRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(-90L, -79L, -68L, -50L, -2L);
        assertThat(firstRightFileRecords).extracting(record -> record.getValues(List.of("key0")).get(0))
                .containsExactly(12L, 14L, 41L, 47L, 83L);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    private void ingestRecords(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            StateStore stateStore,
            int maxNoOfRecordsInMemory,
            long maxNoOfRecordsInLocalStore,
            String ingestLocalWorkingDirectory,
            Stream<String> fileNames,
            Instant stateStoreUpdateTime) throws StateStoreException, IteratorException, IOException {
        ParquetConfiguration parquetConfiguration = parquetConfiguration(
                recordListAndSchema.sleeperSchema, hadoopConfiguration);
        try (IngestCoordinator<Record> ingestCoordinator = standardIngestCoordinator(
                stateStore, recordListAndSchema.sleeperSchema,
                ArrayListRecordBatchFactory.builder()
                        .parquetConfiguration(parquetConfiguration)
                        .localWorkingDirectory(ingestLocalWorkingDirectory)
                        .maxNoOfRecordsInMemory(maxNoOfRecordsInMemory)
                        .maxNoOfRecordsInLocalStore(maxNoOfRecordsInLocalStore)
                        .buildAcceptingRecords(),
                DirectPartitionFileWriterFactory.from(
                        parquetConfiguration,
                        "s3a://" + dataBucketName,
                        fileNames.iterator()::next,
                        () -> stateStoreUpdateTime))) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }
}
