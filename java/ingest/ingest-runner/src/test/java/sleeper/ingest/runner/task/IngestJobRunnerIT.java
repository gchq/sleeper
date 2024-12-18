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
package sleeper.ingest.runner.task;

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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.core.CommonTestConstants;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.core.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.runner.testutils.RecordGenerator;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestAddedFilesStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.runner.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRecordsFromPartitionDataFiles;
import static sleeper.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
class IngestJobRunnerIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(Service.S3, Service.SQS);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard());
    protected final S3AsyncClient s3Async = buildAwsV2Client(localStackContainer, Service.S3, S3AsyncClient.builder());
    protected final AmazonSQS sqs = buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard());
    protected final Configuration hadoopConfiguration = getHadoopConfiguration(localStackContainer);

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final String tableName = "test-table";
    private final String tableId = UUID.randomUUID().toString();
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final String ingestSourceBucketName = "ingest-source-" + UUID.randomUUID().toString();
    private final IngestJobStatusStore statusStore = new InMemoryIngestJobStatusStore();
    @TempDir
    public java.nio.file.Path localDir;
    private Supplier<Instant> timeSupplier = Instant::now;

    @BeforeEach
    public void before() throws IOException {
        s3.createBucket(dataBucketName);
        s3.createBucket(ingestSourceBucketName);
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
    }

    @Test
    void shouldIngestParquetFiles() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 2);
        List<Record> doubledRecords = Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                .flatMap(List::stream).collect(Collectors.toList());

        // When
        runIngestJob(stateStore, recordListAndSchema, files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(doubledRecords);
        assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
    }

    @Test
    void shouldIgnoreFilesOfUnreadableFormats() throws Exception {
        // Given
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 1);
        URI uri1 = new URI("s3a://" + ingestSourceBucketName + "/file-1.crc");
        FileSystem.get(uri1, hadoopConfiguration).createNewFile(new Path(uri1));
        files.add(ingestSourceBucketName + "/file-1.crc");
        URI uri2 = new URI("s3a://" + ingestSourceBucketName + "/file-2.csv");
        FileSystem.get(uri2, hadoopConfiguration).createNewFile(new Path(uri2));
        files.add(ingestSourceBucketName + "/file-2.csv");
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        // When
        runIngestJob(stateStore, recordListAndSchema, files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 200));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
    }

    @Test
    void shouldIngestParquetFilesInNestedDirectories() throws Exception {
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
                        return writeParquetFilesForIngest(recordListAndSchema, dirName, noOfFilesPerDirectory);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).flatMap(List::stream).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        List<Record> expectedRecords = Collections.nCopies(noOfTopLevelDirectories * noOfNestings * noOfFilesPerDirectory, recordListAndSchema.recordList).stream()
                .flatMap(List::stream).collect(Collectors.toList());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        // When
        runIngestJob(stateStore, recordListAndSchema, files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 160));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
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
        writeParquetFileForIngest(new Path("s3a://" + dataBucketName + "/ingest/file1.parquet"), records1);
        writeParquetFileForIngest(new Path("s3a://" + ingestSourceBucketName + "/ingest/file2.parquet"), records2);

        IngestJob ingestJob = IngestJob.builder()
                .tableName(tableName).tableId(tableId).id("id").files(List.of(
                        dataBucketName + "/ingest/file1.parquet",
                        ingestSourceBucketName + "/ingest/file2.parquet"))
                .build();
        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(records1.recordList);
        expectedRecords.addAll(records2.recordList);
        TableProperties tableProperties = createTableProperties(records1.sleeperSchema);
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(records1.sleeperSchema);
        fixTimes(Instant.parse("2024-06-20T15:33:01Z"), Instant.parse("2024-06-20T15:33:10Z"));

        // When
        runIngestJob(tableProperties, stateStore, ingestJob);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(records1.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(stateStore,
                actualFiles.get(0).getLastStateStoreUpdateTime());
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
        assertThat(SketchesDeciles.fromFileReferences(records1.sleeperSchema, actualFiles, hadoopConfiguration))
                .isEqualTo(SketchesDeciles.from(records1.sleeperSchema, expectedRecords));
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
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        TableProperties tableProperties = createTableProperties(recordListAndSchema.sleeperSchema);
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(recordListAndSchema.sleeperSchema);

        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 1);
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .tableId(tableId)
                .id("some-job")
                .files(files)
                .build();
        fixTimes(Instant.parse("2024-06-20T15:10:00Z"), Instant.parse("2024-06-20T15:10:01Z"));

        // When
        runIngestJob(tableProperties, stateStore, job);

        // Then
        List<IngestAddFilesCommitRequest> commitRequests = getCommitRequestsFromQueue();
        List<FileReference> actualFiles = commitRequests.stream()
                .map(IngestAddFilesCommitRequest::getFileReferences)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        List<Record> actualRecords = readMergedRecordsFromPartitionDataFiles(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 10));
        assertThat(actualRecords).containsExactlyInAnyOrderElementsOf(recordListAndSchema.recordList);
        assertThat(SketchesDeciles.fromFileReferences(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration))
                .isEqualTo(SketchesDeciles.from(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList));
        assertThat(commitRequests).containsExactly(IngestAddFilesCommitRequest.builder()
                .ingestJob(job)
                .taskId("test-task")
                .jobRunId("test-job-run")
                .fileReferences(actualFiles)
                .writtenTime(Instant.parse("2024-06-20T15:10:01Z"))
                .build());
    }

    private List<IngestAddFilesCommitRequest> getCommitRequestsFromQueue() {
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
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            List<String> files) throws Exception {
        TableProperties tableProperties = createTableProperties(recordListAndSchema.sleeperSchema);
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .tableId(tableProperties.get(TABLE_ID))
                .id("id")
                .files(files)
                .build();
        runIngestJob(tableProperties, stateStore, job);
    }

    private void runIngestJob(
            TableProperties tableProperties,
            StateStore stateStore,
            IngestJob job) throws Exception {
        statusStore.jobStarted(job.startedEventBuilder(timeSupplier.get()).taskId("test-task").jobRunId("test-job-run").build());
        ingestJobRunner(instanceProperties, tableProperties, stateStore)
                .ingest(job, "test-job-run");
    }

    private IngestJobRunner ingestJobRunner(InstanceProperties instanceProperties,
            TableProperties tableProperties,
            StateStore stateStore) throws Exception {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        StateStoreProvider stateStoreProvider = new FixedStateStoreProvider(tableProperties, stateStore);
        return new IngestJobRunner(
                ObjectFactory.noUserJars(),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider, statusStore,
                "test-task",
                localDir.toString(),
                s3, s3Async, sqs,
                hadoopConfiguration,
                timeSupplier);
    }

    private void fixTimes(Instant... times) {
        timeSupplier = List.of(times).iterator()::next;
    }

    private String createFifoQueueGetUrl() {
        CreateQueueResult result = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")));
        return result.getQueueUrl();
    }

    private TableProperties createTableProperties(Schema schema) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "false");
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    private List<String> writeParquetFilesForIngest(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet",
                    ingestSourceBucketName, subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path("s3a://" + fileWithoutSystemPrefix);
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

}
