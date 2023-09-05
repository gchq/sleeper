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
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.AwsExternalResource.getHadoopConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.standardIngestCoordinator;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;

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

    @BeforeEach
    public void before() {
        s3.createBucket(dataBucketName);
    }

    private StateStore createStateStore(Schema schema) {
        return new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), schema, dynamoDB).create();
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
                List.of("leftFile", "rightFile"),
                List.of(stateStoreUpdateTime, stateStoreUpdateTime));

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileInfoFactory fileInfoFactory = FileInfoFactory.builder()
                .partitionTree(tree)
                .lastStateStoreUpdate(stateStoreUpdateTime)
                .schema(recordListAndSchema.sleeperSchema)
                .build();

        List<FileInfo> fileInfoList = new ArrayList<>();
        fileInfoList.add(fileInfoFactory.partitionFile("right", "s3a://" + dataBucketName + "/partition_right/rightFile.parquet", 100, 0L, 99L));
        fileInfoList.add(fileInfoFactory.partitionFile("left", "s3a://" + dataBucketName + "/partition_left/leftFile.parquet", 100, -100L, -1L));

        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualFiles).containsExactlyInAnyOrderElementsOf(fileInfoList);
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));

        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
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
        List<String> fileNames = new ArrayList<>();
        List<Instant> fileTimes = Collections.nCopies(40, stateStoreUpdateTime);
        for (int i = 0; i < 40; i++) {
            fileNames.add("file" + i);
        }

        // When
        ingestRecords(
                recordListAndSchema,
                stateStore,
                5,
                10L,
                ingestLocalWorkingDirectory,
                fileNames,
                fileTimes);

        // Then
        List<FileInfo> actualFiles = stateStore.getActiveFiles();
        List<Record> actualRecords = new ArrayList<>();
        actualRecords.addAll(readPartitionRecords(actualFiles, "left", recordListAndSchema.sleeperSchema, hadoopConfiguration));
        actualRecords.addAll(readPartitionRecords(actualFiles, "right", recordListAndSchema.sleeperSchema, hadoopConfiguration));
        assertThat(Paths.get(ingestLocalWorkingDirectory)).isEmptyDirectory();
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(actualRecords).extracting(record -> record.getValues(List.of("key0")))
                .containsExactlyElementsOf(LongStream.range(-100, 100).boxed()
                        .map(List::<Object>of)
                        .collect(Collectors.toList()));
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getRowKeyFields().get(0),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration
        );
    }

    private void ingestRecords(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            StateStore stateStore,
            int maxNoOfRecordsInMemory,
            long maxNoOfRecordsInLocalStore,
            String ingestLocalWorkingDirectory,
            List<String> fileNames,
            List<Instant> fileTimes) throws StateStoreException, IteratorException, IOException {
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
                        fileTimes.iterator()::next))) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
    }

    private List<Record> readPartitionRecords(List<FileInfo> fileList, String partition, Schema sleeperSchema, Configuration hadoopConfiguration) {
        List<FileInfo> partitionFiles = new ArrayList<>();
        for (FileInfo file : fileList) {
            if (file.getPartitionId().equals(partition)) {
                partitionFiles.add(file);
            }
        }
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(sleeperSchema, partitionFiles, hadoopConfiguration);
        return actualRecords;
    }
}
