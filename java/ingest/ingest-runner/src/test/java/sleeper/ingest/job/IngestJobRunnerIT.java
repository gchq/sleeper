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
package sleeper.ingest.job;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
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
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStartedEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestAddedFilesStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.ingest.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
class IngestJobRunnerIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(Service.S3, Service.SQS);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard());
    protected final S3AsyncClient s3Async = buildAwsV2Client(localStackContainer, Service.S3, S3AsyncClient.builder());
    protected final AmazonSQS sqs = buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard());
    protected final Configuration hadoopConfiguration = getHadoopConfiguration(localStackContainer);

    private final String instanceId = UUID.randomUUID().toString().substring(0, 18);
    private final String tableName = "test-table";
    private final String tableId = UUID.randomUUID().toString();
    private final String ingestDataBucketName = tableId + "-ingestdata";
    private final String tableDataBucketName = tableId + "-tabledata";
    private final String commitQueue = tableId + "-commit-queue";
    private final IngestJobStatusStore statusStore = new InMemoryIngestJobStatusStore();
    private String commitQueueUrl;
    @TempDir
    public java.nio.file.Path temporaryFolder;
    private String currentLocalIngestDirectory;
    private String currentLocalTableDataDirectory;
    private Supplier<Instant> timeSupplier = Instant::now;

    private static Stream<Arguments> parametersForTests() {
        return Stream.of(
                Arguments.of("arrow", "async", "s3a://"),
                Arguments.of("arrow", "direct", "s3a://"),
                Arguments.of("arrow", "direct", "file://"),
                Arguments.of("arraylist", "async", "s3a://"),
                Arguments.of("arraylist", "direct", "s3a://"),
                Arguments.of("arraylist", "direct", "file://"));
    }

    @BeforeEach
    public void before() throws IOException {
        s3.createBucket(tableDataBucketName);
        s3.createBucket(ingestDataBucketName);
        currentLocalIngestDirectory = createTempDirectory(temporaryFolder, null).toString();
        currentLocalTableDataDirectory = createTempDirectory(temporaryFolder, null).toString();
        commitQueueUrl = createFifoQueueGetUrl(commitQueue);
    }

    private String createFifoQueueGetUrl(String queueName) {
        CreateQueueResult result = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")));
        return result.getQueueUrl();
    }

    private InstanceProperties getInstanceProperties(String fileSystemPrefix,
            String recordBatchType,
            String partitionFileWriterType) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(DATA_BUCKET, getTableDataBucket(fileSystemPrefix));
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, recordBatchType);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, partitionFileWriterType);
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, commitQueueUrl);
        return instanceProperties;
    }

    private TableProperties createTableProperties(Schema schema, String fileSystemPrefix,
            String recordBatchType,
            String partitionFileWriterType) {
        return createTableProperties(schema, getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType));
    }

    private TableProperties createTableProperties(Schema schema, InstanceProperties instanceProperties) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    private String getTableDataBucket(String fileSystemPrefix) {
        switch (fileSystemPrefix.toLowerCase(Locale.ROOT)) {
            case "s3a://":
                return tableDataBucketName;
            case "file://":
                return currentLocalTableDataDirectory;
            default:
                throw new AssertionError(String.format("File system %s is not supported", fileSystemPrefix));
        }
    }

    private String getIngestBucket(String fileSystemPrefix) {
        switch (fileSystemPrefix.toLowerCase(Locale.ROOT)) {
            case "s3a://":
                return ingestDataBucketName;
            case "file://":
                return currentLocalIngestDirectory;
            default:
                throw new AssertionError(String.format("File system %s is not supported", fileSystemPrefix));
        }
    }

    private List<String> writeParquetFilesForIngest(String fileSystemPrefix,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet",
                    getIngestBucket(fileSystemPrefix), subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path(fileSystemPrefix + fileWithoutSystemPrefix);
            writeParquetFileForIngest(path, recordListAndSchema);
        }

        return files;
    }

    private void writeParquetFileForIngest(
            Path path, RecordGenerator.RecordListAndSchema recordListAndSchema) throws IOException {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory
                .createParquetRecordWriter(path, recordListAndSchema.sleeperSchema, hadoopConfiguration);
        for (Record record : recordListAndSchema.recordList) {
            writer.write(record);
        }
        writer.close();
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem: {2}")
    @MethodSource("parametersForTests")
    void shouldIngestParquetFiles(String recordBatchType,
            String partitionFileWriterType,
            String fileSystemPrefix) throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        List<String> files = writeParquetFilesForIngest(
                fileSystemPrefix, recordListAndSchema, "", 2);
        List<Record> doubledRecords = Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                .flatMap(List::stream).collect(Collectors.toList());

        // When
        runIngestJob(
                stateStore,
                fileSystemPrefix,
                recordBatchType,
                partitionFileWriterType,
                recordListAndSchema,
                localDir,
                files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(doubledRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem: {2}")
    @MethodSource("parametersForTests")
    void shouldBeAbleToHandleAllFileFormats(String recordBatchType,
            String partitionFileWriterType,
            String fileSystemPrefix) throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(
                fileSystemPrefix, recordListAndSchema, "", 1);
        String ingestBucket = getIngestBucket(fileSystemPrefix);
        URI uri1 = new URI(fileSystemPrefix + ingestBucket + "/file-1.crc");
        FileSystem.get(uri1, hadoopConfiguration).createNewFile(new Path(uri1));
        files.add(ingestBucket + "/file-1.crc");
        URI uri2 = new URI(fileSystemPrefix + ingestBucket + "/file-2.csv");
        FileSystem.get(uri2, hadoopConfiguration).createNewFile(new Path(uri2));
        files.add(ingestBucket + "/file-2.csv");
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        // When
        runIngestJob(
                stateStore,
                fileSystemPrefix,
                recordBatchType,
                partitionFileWriterType,
                recordListAndSchema,
                localDir,
                files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 200));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @ParameterizedTest(name = "backedBy: {0}, writeMode: {1}, fileSystem:{2}")
    @MethodSource("parametersForTests")
    void shouldIngestParquetFilesInNestedDirectories(String recordBatchType,
            String partitionFileWriterType,
            String fileSystemPrefix) throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        int noOfTopLevelDirectories = 2;
        int noOfNestings = 4;
        int noOfFilesPerDirectory = 2;
        List<String> files = IntStream.range(0, noOfTopLevelDirectories)
                .mapToObj(topLevelDirNo -> IntStream.range(0, noOfNestings).mapToObj(nestingNo -> {
                    try {
                        String dirName = String.format("dir-%d%s", topLevelDirNo, String.join("", Collections.nCopies(nestingNo, "/nested-dir")));
                        return writeParquetFilesForIngest(
                                fileSystemPrefix, recordListAndSchema, dirName, noOfFilesPerDirectory);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).flatMap(List::stream).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        List<Record> expectedRecords = Collections.nCopies(noOfTopLevelDirectories * noOfNestings * noOfFilesPerDirectory, recordListAndSchema.recordList).stream()
                .flatMap(List::stream).collect(Collectors.toList());
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        // When
        runIngestJob(
                stateStore,
                fileSystemPrefix,
                recordBatchType,
                partitionFileWriterType,
                recordListAndSchema,
                localDir,
                files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 160));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
    }

    @Test
    void shouldWriteRecordsFromTwoBuckets() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema records1 = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        RecordGenerator.RecordListAndSchema records2 = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(10, 20).boxed().collect(Collectors.toList()));
        writeParquetFileForIngest(new Path("s3a://" + tableDataBucketName + "/ingest/file1.parquet"), records1);
        writeParquetFileForIngest(new Path("s3a://" + ingestDataBucketName + "/ingest/file2.parquet"), records2);

        IngestJob ingestJob = IngestJob.builder()
                .tableName(tableName).tableId(tableId).id("id").files(List.of(
                        tableDataBucketName + "/ingest/file1.parquet",
                        ingestDataBucketName + "/ingest/file2.parquet"))
                .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(records1.recordList);
        expectedRecords.addAll(records2.recordList);
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties("s3a://", "arrow", "async");
        TableProperties tableProperties = createTableProperties(records1.sleeperSchema, "s3a://", "arrow", "async");
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(records1.sleeperSchema);
        fixTimes(Instant.parse("2024-06-20T15:33:01Z"), Instant.parse("2024-06-20T15:33:10Z"));

        // When
        runIngestJob(instanceProperties, tableProperties, stateStore, localDir, ingestJob);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(records1.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(stateStore,
                actualFiles.get(0).getLastStateStoreUpdateTime());
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        ResultVerifier.assertOnSketch(
                new Field("key0", new LongType()),
                new RecordGenerator.RecordListAndSchema(expectedRecords, records1.sleeperSchema),
                actualFiles,
                hadoopConfiguration);
        assertThat(statusStore.getAllJobs(tableId)).containsExactly(
                jobStatus(ingestJob, ProcessRun.builder()
                        .taskId("test-task")
                        .startedStatus(ingestStartedStatus(ingestJob, Instant.parse("2024-06-20T15:33:01Z")))
                        .statusUpdate(ingestAddedFilesStatus(Instant.parse("2024-06-20T15:33:10Z"), 1))
                        .build()));
    }

    @Test
    void shouldCommitFilesAsynchronously() throws Exception {
        // Given
        String fileSystemPrefix = "s3a://";
        String recordBatchType = "arrow";
        String partitionFileWriterType = "async";
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TableProperties tableProperties = createTableProperties(recordListAndSchema.sleeperSchema, instanceProperties);
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        List<String> files = writeParquetFilesForIngest(fileSystemPrefix, recordListAndSchema, "", 1);
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .tableId(tableId)
                .id("some-job")
                .files(files)
                .build();
        fixTimes(Instant.parse("2024-06-20T15:10:01Z"));

        // When
        runIngestJob(instanceProperties, tableProperties, stateStore, localDir, job);

        // Then
        List<IngestAddFilesCommitRequest> commitRequests = getCommitRequestsFromQueue(instanceProperties);
        List<FileReference> actualFiles = commitRequests.stream()
                .map(IngestAddFilesCommitRequest::getFileReferences)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(Paths.get(localDir)).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 10));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        ResultVerifier.assertOnSketch(
                recordListAndSchema.sleeperSchema.getField("key0").orElseThrow(),
                recordListAndSchema,
                actualFiles,
                hadoopConfiguration);
        assertThat(commitRequests).containsExactly(IngestAddFilesCommitRequest.builder()
                .ingestJob(job)
                .taskId("test-task")
                .jobRunId("test-job-run")
                .fileReferences(actualFiles)
                .writtenTime(Instant.parse("2024-06-20T15:10:01Z"))
                .build());
    }

    private List<IngestAddFilesCommitRequest> getCommitRequestsFromQueue(InstanceProperties instanceProperties) {
        String commitQueueUrl = instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL);
        ReceiveMessageResult result = sqs.receiveMessage(commitQueueUrl);
        IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();
        return result.getMessages().stream()
                .map(Message::getBody)
                .map(serDe::fromJson)
                .collect(Collectors.toList());
    }

    private void runIngestJob(
            StateStore stateStore,
            String fileSystemPrefix,
            String recordBatchType,
            String partitionFileWriterType,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String localDir,
            List<String> files) throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties(fileSystemPrefix, recordBatchType, partitionFileWriterType);
        TableProperties tableProperties = createTableProperties(recordListAndSchema.sleeperSchema, fileSystemPrefix, recordBatchType, partitionFileWriterType);
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .tableId(tableProperties.get(TABLE_ID))
                .id("id")
                .files(files)
                .build();
        runIngestJob(instanceProperties, tableProperties, stateStore, localDir, job);
    }

    private void runIngestJob(InstanceProperties instanceProperties,
            TableProperties tableProperties,
            StateStore stateStore,
            String localDir,
            IngestJob job) throws Exception {
        statusStore.jobStarted(IngestJobStartedEvent.ingestJobStarted(job, timeSupplier.get()).taskId("test-task").jobRunId("test-job-run").build());
        ingestJobRunner(instanceProperties, tableProperties, stateStore, localDir)
                .ingest(job, "test-job-run");
    }

    private IngestJobRunner ingestJobRunner(InstanceProperties instanceProperties,
            TableProperties tableProperties,
            StateStore stateStore,
            String localDir) throws Exception {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tableProperties, stateStore);
        return new IngestJobRunner(
                new ObjectFactory(instanceProperties, null, createTempDirectory(temporaryFolder, null).toString()),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider, statusStore,
                "test-task",
                localDir,
                s3Async, sqs,
                hadoopConfiguration,
                timeSupplier);
    }

    private void fixTimes(Instant... times) {
        timeSupplier = new LinkedList<>(List.of(times))::poll;
    }
}
