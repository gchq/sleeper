/*
 * Copyright 2022-2025 Crown Copyright
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.utils.S3FileNotFoundException;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.runner.testutils.RowGenerator;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestStartedStatus;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRowsFromPartitionDataFiles;

class IngestJobRunnerIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final String tableName = "test-table";
    private final String tableId = UUID.randomUUID().toString();
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final String ingestSourceBucketName = "ingest-source-" + UUID.randomUUID().toString();
    private final IngestJobTracker tracker = new InMemoryIngestJobTracker();
    private final SketchesStore sketchesStore = new S3SketchesStore(s3Client, s3TransferManager);
    @TempDir
    public java.nio.file.Path localDir;
    private Supplier<Instant> timeSupplier = Instant::now;

    @BeforeEach
    public void before() throws IOException {
        createBucket(dataBucketName);
        createBucket(ingestSourceBucketName);
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "false");
    }

    @Test
    void shouldIngestParquetFiles() throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        StateStore stateStore = initialiseStateStore();

        List<String> files = writeParquetFilesForIngest(rowListAndSchema, 2);
        List<Row> doubledRows = Stream.of(rowListAndSchema.rowList, rowListAndSchema.rowList)
                .flatMap(List::stream).collect(Collectors.toList());

        // When
        runIngestJob(stateStore, files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(doubledRows);
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @Test
    void shouldIgnoreFilesOfUnreadableFormats() throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        List<String> files = writeParquetFilesForIngest(rowListAndSchema, 1);
        URI uri1 = new URI("s3a://" + ingestSourceBucketName + "/file-1.crc");
        FileSystem.get(uri1, hadoopConf).createNewFile(new Path(uri1));
        files.add(ingestSourceBucketName + "/file-1.crc");
        URI uri2 = new URI("s3a://" + ingestSourceBucketName + "/file-2.csv");
        FileSystem.get(uri2, hadoopConf).createNewFile(new Path(uri2));
        files.add(ingestSourceBucketName + "/file-2.csv");
        StateStore stateStore = initialiseStateStore();

        // When
        assertThatExceptionOfType(S3FileNotFoundException.class)
                .isThrownBy(() -> runIngestJob(stateStore, files));

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles).isEmpty();
        assertThat(actualRows).isEmpty();
    }

    @Test
    void shouldIngestParquetFilesInNestedDirectories() throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        int noOfTopLevelDirectories = 2;
        int noOfNestings = 4;
        int noOfFilesPerDirectory = 2;
        List<String> files = IntStream.range(0, noOfTopLevelDirectories)
                .mapToObj(topLevelDirNo -> IntStream.range(0, noOfNestings).mapToObj(nestingNo -> {
                    try {
                        String dirName = String.format("dir-%d%s", topLevelDirNo, String.join("", Collections.nCopies(nestingNo, "/nested-dir")));
                        return writeParquetFilesForIngest(rowListAndSchema, dirName, noOfFilesPerDirectory);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).flatMap(List::stream).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        List<Row> expectedRows = Collections.nCopies(noOfTopLevelDirectories * noOfNestings * noOfFilesPerDirectory, rowListAndSchema.rowList).stream()
                .flatMap(List::stream).collect(Collectors.toList());
        StateStore stateStore = initialiseStateStore();

        // When
        runIngestJob(stateStore, files);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 160));
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @Test
    void shouldWriteRowsFromTwoBuckets() throws Exception {
        // Given
        RowGenerator.RowListAndSchema rows1 = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        RowGenerator.RowListAndSchema rows2 = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(10, 20).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rows1.sleeperSchema);
        writeParquetFileForIngest(new Path("s3a://" + dataBucketName + "/ingest/file1.parquet"), rows1);
        writeParquetFileForIngest(new Path("s3a://" + ingestSourceBucketName + "/ingest/file2.parquet"), rows2);

        IngestJob ingestJob = IngestJob.builder()
                .tableName(tableName).tableId(tableId).id("id").files(List.of(
                        dataBucketName + "/ingest/file1.parquet",
                        ingestSourceBucketName + "/ingest/file2.parquet"))
                .build();
        List<Row> expectedRows = new ArrayList<>();
        expectedRows.addAll(rows1.rowList);
        expectedRows.addAll(rows2.rowList);
        StateStore stateStore = initialiseStateStore();
        fixTimes(Instant.parse("2024-06-20T15:33:01Z"), Instant.parse("2024-06-20T15:33:10Z"));

        // When
        runIngestJob(stateStore, ingestJob);

        // Then
        List<FileReference> actualFiles = stateStore.getFileReferences();
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rows1.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(stateStore,
                actualFiles.get(0).getLastStateStoreUpdateTime());
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 20));
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
        assertThat(SketchesDeciles.fromFileReferences(rows1.sleeperSchema, actualFiles, sketchesStore))
                .isEqualTo(SketchesDeciles.from(rows1.sleeperSchema, expectedRows));
        assertThat(tracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus(ingestJob, jobRunOnTask("test-task",
                        ingestStartedStatus(ingestJob, Instant.parse("2024-06-20T15:33:01Z")),
                        ingestAddedFilesStatus(Instant.parse("2024-06-20T15:33:10Z"), 1))));
    }

    @Test
    void shouldCommitFilesAsynchronously() throws Exception {
        // Given
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-5, 5).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        tableProperties.set(INGEST_FILES_COMMIT_ASYNC, "true");
        StateStore stateStore = initialiseStateStore();

        List<String> files = writeParquetFilesForIngest(rowListAndSchema, 1);
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .tableId(tableId)
                .id("some-job")
                .files(files)
                .build();
        fixTimes(Instant.parse("2024-06-20T15:10:00Z"), Instant.parse("2024-06-20T15:10:01Z"));

        // When
        runIngestJob(stateStore, job);

        // Then
        List<StateStoreCommitRequest> commitRequests = getCommitRequestsFromQueue(tableProperties);
        List<FileReference> actualFiles = getFilesAdded(commitRequests);
        List<Row> actualRows = readMergedRowsFromPartitionDataFiles(rowListAndSchema.sleeperSchema, actualFiles, hadoopConf);
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        assertThat(localDir).isEmptyDirectory();
        assertThat(actualFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile("anyfilename", 10));
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rowListAndSchema.rowList);
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFiles, sketchesStore))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
        assertThat(commitRequests).containsExactly(StateStoreCommitRequest.create(tableId,
                AddFilesTransaction.builder()
                        .jobId(job.getId()).taskId("test-task").jobRunId("test-job-run")
                        .writtenTime(Instant.parse("2024-06-20T15:10:01Z"))
                        .files(AllReferencesToAFile.newFilesWithReferences(actualFiles))
                        .build()));
    }

    private List<StateStoreCommitRequest> getCommitRequestsFromQueue(TableProperties tableProperties) {
        StateStoreCommitRequestSerDe serDe = new StateStoreCommitRequestSerDe(tableProperties);
        return receiveMessages(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .map(serDe::fromJson)
                .toList();
    }

    private List<FileReference> getFilesAdded(List<StateStoreCommitRequest> commitRequests) {
        return commitRequests.stream().flatMap(this::streamFilesAdded).toList();
    }

    private Stream<FileReference> streamFilesAdded(StateStoreCommitRequest commitRequest) {
        AddFilesTransaction transaction = commitRequest.<AddFilesTransaction>getTransactionIfHeld().orElseThrow();
        return transaction.getFiles().stream().flatMap(file -> file.getReferences().stream());
    }

    private void runIngestJob(
            StateStore stateStore,
            List<String> files) throws Exception {
        IngestJob job = IngestJob.builder()
                .tableName(tableName)
                .tableId(tableProperties.get(TABLE_ID))
                .id("id")
                .files(files)
                .build();
        runIngestJob(stateStore, job);
    }

    private void runIngestJob(StateStore stateStore, IngestJob job) throws Exception {
        tracker.jobStarted(job.startedEventBuilder(timeSupplier.get()).taskId("test-task").jobRunId("test-job-run").build());
        ingestJobRunner(stateStore).ingest(job, "test-job-run");
    }

    private IngestJobRunner ingestJobRunner(StateStore stateStore) throws Exception {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        StateStoreProvider stateStoreProvider = FixedStateStoreProvider.singleTable(tableProperties, stateStore);
        return new IngestJobRunner(
                ObjectFactory.noUserJars(),
                instanceProperties,
                tablePropertiesProvider,
                PropertiesReloader.neverReload(),
                stateStoreProvider, tracker,
                "test-task",
                localDir.toString(),
                s3Client, s3AsyncClient, sqsClient,
                hadoopConf,
                timeSupplier);
    }

    private void fixTimes(Instant... times) {
        timeSupplier = List.of(times).iterator()::next;
    }

    private List<String> writeParquetFilesForIngest(
            RowGenerator.RowListAndSchema rowListAndSchema,
            int numberOfFiles) throws IOException {
        return writeParquetFilesForIngestWithRoot(rowListAndSchema, ingestSourceBucketName, numberOfFiles);
    }

    private List<String> writeParquetFilesForIngest(
            RowGenerator.RowListAndSchema rowListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        return writeParquetFilesForIngestWithRoot(rowListAndSchema, ingestSourceBucketName + "/" + subDirectory, numberOfFiles);
    }

    private List<String> writeParquetFilesForIngestWithRoot(
            RowGenerator.RowListAndSchema rowListAndSchema,
            String rootDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/file-%d.parquet", rootDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path("s3a://" + fileWithoutSystemPrefix);
            writeParquetFileForIngest(path, rowListAndSchema);
        }

        return files;
    }

    private void writeParquetFileForIngest(
            Path path, RowGenerator.RowListAndSchema rowListAndSchema) throws IOException {
        ParquetWriter<Row> writer = ParquetRowWriterFactory
                .createParquetRowWriter(path, rowListAndSchema.sleeperSchema, hadoopConf);
        for (Row row : rowListAndSchema.rowList) {
            writer.write(row);
        }
        writer.close();
    }

    private StateStore initialiseStateStore() {
        return InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());
    }

}
