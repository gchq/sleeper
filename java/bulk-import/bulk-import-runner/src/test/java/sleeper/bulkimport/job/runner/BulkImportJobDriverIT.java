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
package sleeper.bulkimport.job.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.BulkImportJobDriver.AddFilesAsynchronously;
import sleeper.bulkimport.job.runner.dataframe.BulkImportJobDataframeDriver;
import sleeper.bulkimport.job.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver;
import sleeper.bulkimport.job.runner.rdd.BulkImportJobRDDDriver;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3SerDe;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.s3.S3StateStoreCreator;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_FILES_COMMIT_ASYNC;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.statestore.commit.StateStoreCommitRequestInS3.createFileS3Key;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestAcceptedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestFinishedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestFinishedStatusUncommitted;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.validatedIngestStartedStatus;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
class BulkImportJobDriverIT {

    private static Stream<Arguments> getParameters() {
        return Stream.of(
                Arguments.of(Named.of("BulkImportJobDataframeDriver",
                        (BulkImportJobRunner) BulkImportJobDataframeDriver::createFileReferences)),
                Arguments.of(Named.of("BulkImportJobRDDDriver",
                        (BulkImportJobRunner) BulkImportJobRDDDriver::createFileReferences)),
                Arguments.of(Named.of("BulkImportDataframeLocalSortDriver",
                        (BulkImportJobRunner) BulkImportDataframeLocalSortDriver::createFileReferences)));
    }

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            Service.DYNAMODB, Service.S3, Service.SQS);

    @TempDir
    public java.nio.file.Path folder;
    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDBClient = buildAwsV1Client(localStackContainer, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqsClient = buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard());
    private final Schema schema = getSchema();
    private final IngestJobStatusStore statusStore = new InMemoryIngestJobStatusStore();
    private final String taskId = "test-bulk-import-spark-cluster";
    private final String jobRunId = "test-run";
    private final Instant validationTime = Instant.parse("2023-04-05T16:00:01Z");
    private final Instant startTime = Instant.parse("2023-04-05T16:01:01Z");
    private final Instant writtenTime = Instant.parse("2023-04-05T16:01:10Z");
    private final Instant endTime = Instant.parse("2023-04-05T16:01:11Z");
    private final Configuration conf = getHadoopConfiguration(localStackContainer);
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String dataDir;

    @BeforeAll
    public static void setSparkProperties() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.app.name", "bulk import");
    }

    @AfterAll
    public static void clearSparkProperties() {
        System.clearProperty("spark.master");
        System.clearProperty("spark.app.name");
    }

    @BeforeEach
    void setUp() {
        dataDir = folder.toString();
        instanceProperties = createInstanceProperties(s3Client, dataDir);
        tableProperties = createTableProperties(instanceProperties);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataSinglePartition(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Record> readRecords = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileReference.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                assertThat(recordsInThisFile).isSortedAccordingTo(new RecordComparator(getSchema()));
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
        IngestJob ingestJob = job.toIngestJob();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, endTime, 200, 200), 1))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataSinglePartitionIdenticalRowKeyDifferentSortKeys(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecordsIdenticalRowKey();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Record> readRecords = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileReference.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                assertThat(recordsInThisFile).isSortedAccordingTo(new RecordComparator(getSchema()));
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
        IngestJob ingestJob = job.toIngestJob();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, endTime, 100, 100), 1))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataMultiplePartitions(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties, Collections.singletonList(50));

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<Record> leftPartition = records.stream()
                .filter(record -> ((int) record.get("key")) < 50)
                .collect(Collectors.toList());
        sortRecords(leftPartition);
        List<Record> rightPartition = records.stream()
                .filter(record -> ((int) record.get("key")) >= 50)
                .collect(Collectors.toList());
        sortRecords(rightPartition);
        assertThat(stateStore.getFileReferences())
                .extracting(FileReference::getNumberOfRecords,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactlyInAnyOrder(
                        tuple(100L, leftPartition),
                        tuple(100L, rightPartition));
        IngestJob ingestJob = job.toIngestJob();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, endTime, 200, 200), 2))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportLargeAmountOfDataMultiplePartitions(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        // - Write some data to be imported
        List<Record> records = getLotsOfRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties, getSplitPointsForLotsOfRecords());

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        for (Partition leaf : leafPartitions) {
            Integer minRowKey = (Integer) leaf.getRegion().getRange(schema.getRowKeyFieldNames().get(0)).getMin();
            if (Integer.MIN_VALUE == minRowKey) {
                continue;
            }
            List<FileReference> relevantFiles = fileReferences.stream()
                    .filter(af -> af.getPartitionId().equals(leaf.getId()))
                    .collect(Collectors.toList());

            long totalRecords = relevantFiles.stream()
                    .map(FileReference::getNumberOfRecords)
                    .reduce(Long::sum)
                    .orElseThrow();

            assertThat(totalRecords).isEqualTo(2000L);

            relevantFiles.stream()
                    .map(af -> {
                        try {
                            return new ParquetRecordReader(new Path(af.getFilename()), schema);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(reader -> {
                        List<Record> recordsRead = new ArrayList<>();
                        Record record;
                        try {
                            record = reader.read();
                            while (record != null) {
                                recordsRead.add(record);
                                record = reader.read();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return recordsRead;
                    })
                    .forEach(read -> assertThat(read).isSortedAccordingTo(new RecordComparator(getSchema())));
        }
        IngestJob ingestJob = job.toIngestJob();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, endTime, 100000, 100000), 50))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldNotThrowExceptionIfProvidedWithDirectoryWhichContainsParquetAndNonParquetFiles(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        // - Write a dummy file
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dataDir + "/import/b.txt", StandardCharsets.UTF_8))) {
            bufferedWriter.append("test");
        }
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job")
                .files(Lists.newArrayList(dataDir + "/import/")).build();
        runJob(runner, instanceProperties, job);

        // Then
        String expectedPartitionId = stateStore.getAllPartitions().get(0).getId();
        sortRecords(records);
        assertThat(stateStore.getFileReferences())
                .extracting(FileReference::getNumberOfRecords, FileReference::getPartitionId,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactly(tuple(200L, expectedPartitionId, records));
        IngestJob ingestJob = job.toIngestJob();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, endTime, 200, 200), 1))
                        .build()));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void shouldImportDataWithS3StateStore(BulkImportJobRunner runner) throws IOException, StateStoreException {
        // Given
        // - Set state store type
        tableProperties.set(STATESTORE_CLASSNAME, S3StateStore.class.getName());
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(runner, instanceProperties, job);

        // Then
        List<FileReference> fileReferences = stateStore.getFileReferences();
        List<Record> readRecords = new ArrayList<>();
        for (FileReference fileReference : fileReferences) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileReference.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                assertThat(recordsInThisFile).isSortedAccordingTo(new RecordComparator(getSchema()));
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
        IngestJob ingestJob = job.toIngestJob();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, endTime, 200, 200), 1))
                        .build()));
    }

    @Test
    void shouldImportDataWithAsynchronousCommitOfNewFiles() throws Exception {
        // Given
        // - Set async commit
        tableProperties.set(BULK_IMPORT_FILES_COMMIT_ASYNC, "true");
        // - Write some data to be imported
        List<Record> records = getRecords();
        writeRecordsToFile(records, dataDir + "/import/a.parquet");
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add(dataDir + "/import/a.parquet");
        // - State store
        StateStore stateStore = createTable(instanceProperties, tableProperties);

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(inputFiles).build();
        runJob(BulkImportJobDataframeDriver::createFileReferences, instanceProperties, job, startWrittenAndEndTime());

        // Then
        IngestJob ingestJob = job.toIngestJob();
        assertThat(stateStore.getFileReferences()).isEmpty();
        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        assertThat(receiveAddFilesCommitMessages()).singleElement().satisfies(commit -> {
            assertThat(commit).isEqualTo(IngestAddFilesCommitRequest.builder()
                    .ingestJob(ingestJob)
                    .fileReferences(commit.getFileReferences())
                    .taskId(taskId).jobRunId(jobRunId)
                    .writtenTime(writtenTime)
                    .build());
            assertThat(commit.getFileReferences())
                    .extracting(FileReference::getNumberOfRecords, FileReference::getPartitionId,
                            file -> readRecords(file.getFilename(), schema))
                    .containsExactly(tuple(200L, "root", expectedRecords));
        });
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID)))
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId(taskId)
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatusUncommitted(ingestJob,
                                summary(startTime, endTime, 200, 200), 1))
                        .build()));
    }

    @Test
    void shouldSendAsynchronousCommitWithTooManyFilesForSqs() throws Exception {
        // Given
        instanceProperties.set(DATA_BUCKET, "test-data-bucket-" + UUID.randomUUID().toString());
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        StateStore stateStore = createTable(instanceProperties, tableProperties);
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = IntStream.range(0, 1350)
                .mapToObj(i -> factory.rootFile("s3a://test-data-bucket/test-table/data/partition_root/test-file" + i + ".parquet", 100L))
                .collect(Collectors.toList());
        Supplier<String> s3FileNameSupplier = () -> "test-add-files-commit";

        // When
        BulkImportJob job = jobForTable(tableProperties).id("my-job").files(List.of("test-input-file.parquet")).build();
        BulkImportJobDriver.submitFilesToCommitQueue(sqsClient, s3Client, instanceProperties, s3FileNameSupplier).submit(
                IngestAddFilesCommitRequest.builder()
                        .ingestJob(job.toIngestJob())
                        .fileReferences(fileReferences)
                        .taskId(taskId).jobRunId(jobRunId)
                        .writtenTime(writtenTime)
                        .build());

        // Then
        String expectedS3Key = createFileS3Key(tableProperties.get(TABLE_ID), "test-add-files-commit");
        assertThat(receiveCommitRequestStoredInS3Messages())
                .containsExactly(new StateStoreCommitRequestInS3(expectedS3Key));
        assertThat(readAddFilesCommitRequestFromDataBucket(expectedS3Key))
                .isEqualTo(IngestAddFilesCommitRequest.builder()
                        .ingestJob(job.toIngestJob())
                        .fileReferences(fileReferences)
                        .taskId(taskId).jobRunId(jobRunId)
                        .writtenTime(writtenTime)
                        .build());
    }

    private static List<Record> readRecords(String filename, Schema schema) {
        try (ParquetRecordReader reader = new ParquetRecordReader(new Path(filename), schema)) {
            List<Record> readRecords = new ArrayList<>();
            Record record = reader.read();
            while (null != record) {
                readRecords.add(new Record(record));
                record = reader.read();
            }
            return readRecords;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading records", e);
        }
    }

    private static void sortRecords(List<Record> records) {
        RecordComparator recordComparator = new RecordComparator(getSchema());
        records.sort(recordComparator);
    }

    public InstanceProperties createInstanceProperties(AmazonS3 s3Client, String dir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DATA_BUCKET, dir);
        instanceProperties.set(FILE_SYSTEM, "file://");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        return instanceProperties;
    }

    public TableProperties createTableProperties(InstanceProperties instanceProperties) {
        return createTestTableProperties(instanceProperties, schema);
    }

    private TablePropertiesStore tablePropertiesStore(InstanceProperties instanceProperties) {
        return S3TableProperties.getStore(instanceProperties, s3Client, dynamoDBClient);
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(
                        new Field("value1", new StringType()),
                        new Field("value2", new ListType(new IntType())),
                        new Field("value3", new MapType(new StringType(), new LongType())))
                .build();
    }

    private static List<Record> getRecords() {
        List<Record> records = new ArrayList<>(200);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
            // Add record again but with the sort field set to a different value
            Record record2 = new Record(record);
            record2.put("sort", ((long) record.get("sort")) - 1L);
            records.add(record2);
        }
        Collections.shuffle(records);
        return records;
    }

    private static List<Record> getRecordsIdenticalRowKey() {
        List<Record> records = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", 1);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
        }
        Collections.shuffle(records);
        return records;
    }

    private static List<Record> getLotsOfRecords() {
        List<Record> records = new ArrayList<>(100000);
        for (int i = 0; i < 50000; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
            // Add record again but with the sort field set to a different value
            Record record2 = new Record(record);
            record2.put("sort", ((long) record.get("sort")) - 1L);
            records.add(record2);
        }
        Collections.shuffle(records);
        return records;
    }

    private static List<Object> getSplitPointsForLotsOfRecords() {
        List<Object> splitPoints = new ArrayList<>();
        for (int i = 0; i < 50000; i++) {
            if (i % 1000 == 0) {
                splitPoints.add(i);
            }
        }
        return splitPoints;
    }

    private static void writeRecordsToFile(List<Record> records, String file) throws IllegalArgumentException, IOException {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file), getSchema());
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
    }

    private StateStore createTable(InstanceProperties instanceProperties, TableProperties tableProperties, List<Object> splitPoints) throws StateStoreException {
        tablePropertiesStore(instanceProperties).save(tableProperties);
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, conf).getStateStore(tableProperties);
        stateStore.initialise(new PartitionsFromSplitPoints(getSchema(), splitPoints).construct());
        return stateStore;
    }

    private StateStore createTable(InstanceProperties instanceProperties, TableProperties tableProperties) throws StateStoreException {
        return createTable(instanceProperties, tableProperties, Collections.emptyList());
    }

    private void runJob(BulkImportJobRunner runner, InstanceProperties properties, BulkImportJob job) throws IOException {
        runJob(runner, instanceProperties, job, startAndEndTime());
    }

    private void runJob(BulkImportJobRunner runner, InstanceProperties properties, BulkImportJob job, Supplier<Instant> timeSupplier) throws IOException {
        statusStore.jobValidated(ingestJobAccepted(job.toIngestJob(), validationTime).jobRunId(jobRunId).build());
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(instanceProperties, s3Client, dynamoDBClient, conf);
        AddFilesAsynchronously addFilesAsync = BulkImportJobDriver.submitFilesToCommitQueue(sqsClient, s3Client, instanceProperties);
        BulkImportJobDriver driver = new BulkImportJobDriver(new BulkImportSparkSessionRunner(
                runner, instanceProperties, tablePropertiesProvider, stateStoreProvider),
                tablePropertiesProvider, stateStoreProvider, statusStore, addFilesAsync, timeSupplier);
        driver.run(job, jobRunId, taskId);
    }

    private Supplier<Instant> startAndEndTime() {
        return List.of(startTime, endTime).iterator()::next;
    }

    private Supplier<Instant> startWrittenAndEndTime() {
        return List.of(startTime, writtenTime, endTime).iterator()::next;
    }

    private BulkImportJob.Builder jobForTable(TableProperties tableProperties) {
        return BulkImportJob.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME));
    }

    private List<StateStoreCommitRequestInS3> receiveCommitRequestStoredInS3Messages() {
        return receiveCommitMessages().stream()
                .map(message -> new StateStoreCommitRequestInS3SerDe().fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<IngestAddFilesCommitRequest> receiveAddFilesCommitMessages() {
        return receiveCommitMessages().stream()
                .map(message -> new IngestAddFilesCommitRequestSerDe().fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<Message> receiveCommitMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqsClient.receiveMessage(receiveMessageRequest).getMessages();
    }

    private IngestAddFilesCommitRequest readAddFilesCommitRequestFromDataBucket(String s3Key) {
        String requestJson = s3Client.getObjectAsString(instanceProperties.get(DATA_BUCKET), s3Key);
        return new IngestAddFilesCommitRequestSerDe().fromJson(requestJson);
    }

    private String createFifoQueueGetUrl() {
        CreateQueueResult result = sqsClient.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")));
        return result.getQueueUrl();
    }
}
