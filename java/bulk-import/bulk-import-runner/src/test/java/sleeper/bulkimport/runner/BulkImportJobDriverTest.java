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

package sleeper.bulkimport.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.testutils.printers.PartitionsPrinter;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.extend.InsufficientDataForPartitionSplittingException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.defaultFileOnRootPartitionWithRows;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAcceptedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatusUncommitted;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.validatedIngestStartedStatus;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.failedStatus;

class BulkImportJobDriverTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final InMemoryTransactionLogStore filesLogStore = transactionLogs.getFilesLogStore();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);
    private final IngestJobTracker tracker = new InMemoryIngestJobTracker();
    private final List<StateStoreCommitRequest> commitRequestQueue = new ArrayList<>();
    private final List<FakeBulkImportContext> jobContextsCreated = new ArrayList<>();
    private final List<FakeBulkImportContext> jobContextsClosed = new ArrayList<>();
    private final Map<String, Sketches> partitionIdToSketches = new HashMap<>();

    @BeforeEach
    void setUp() {
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 1);
        stateStore.fixFileUpdateTime(null);
    }

    @Nested
    @DisplayName("Report results of bulk import")
    class ReportResults {

        @Test
        void shouldReportJobFinished() throws Exception {
            // Given
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
            Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
            List<FileReference> outputFiles = List.of(
                    defaultFileOnRootPartitionWithRows("test-output.parquet", 100));

            // When
            runJob(job, "test-run", "test-task", validationTime,
                    driver(successfulWithOutput(outputFiles), startFinishAndFailureTime(startTime, finishTime, finishTime)));

            // Then
            assertThat(allJobsReported())
                    .containsExactly(ingestJobStatus(job.getId(), jobRunOnTask("test-task",
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(startTime, 1),
                            ingestFinishedStatus(summary(startTime, finishTime, 100, 100), 1))));
            assertThat(stateStore.getFileReferences())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .isEqualTo(outputFiles);
            assertThat(commitRequestQueue).isEmpty();
            assertThat(jobContextsClosed).extracting(FakeBulkImportContext::job).containsExactly(job);
        }

        @Test
        void shouldReportJobFailed() throws Exception {
            // Given
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
            Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
            RuntimeException rootCause = new RuntimeException("Root cause");
            RuntimeException cause = new RuntimeException("Some cause", rootCause);
            RuntimeException jobFailure = new RuntimeException("Failed running job", cause);

            // When
            var driver = driver(
                    failWithException(jobFailure), startAndFinishTime(startTime, finishTime));
            assertThatThrownBy(() -> runJob(job, "test-run", "test-task", validationTime, driver))
                    .isSameAs(jobFailure);

            // Then
            assertThat(allJobsReported())
                    .containsExactly(ingestJobStatus(job.getId(), jobRunOnTask("test-task",
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(startTime, 1),
                            failedStatus(finishTime, List.of("Failed running job", "Some cause", "Root cause")))));
            assertThat(stateStore.getFileReferences()).isEmpty();
            assertThat(commitRequestQueue).isEmpty();
            assertThat(jobContextsClosed).extracting(FakeBulkImportContext::job).containsExactly(job);
        }

        @Test
        void shouldReportStateStoreUpdateFailed() throws Exception {
            // Given
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
            Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
            RuntimeException jobFailure = new RuntimeException("Failed updating files");
            List<FileReference> outputFiles = List.of(
                    defaultFileOnRootPartitionWithRows("test-output.parquet", 100));
            filesLogStore.atStartOfNextAddTransaction(() -> {
                throw jobFailure;
            });

            // When
            var driver = driver(
                    successfulWithOutput(outputFiles), stateStore, startFinishAndFailureTime(startTime, finishTime, finishTime));
            assertThatThrownBy(() -> runJob(job, "test-run", "test-task", validationTime, driver))
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(StateStoreException.class)
                    .cause().isSameAs(jobFailure);

            // Then
            assertThat(allJobsReported())
                    .containsExactly(ingestJobStatus(job.getId(), jobRunOnTask("test-task",
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(startTime, 1),
                            failedStatus(finishTime, List.of(
                                    "Failed to add files to state store. Ensure this service account has write access. " +
                                            "Files may need to be re-imported for clients to access data.",
                                    "Failed adding transaction", "Failed updating files")))));
            assertThat(commitRequestQueue).isEmpty();
            assertThat(jobContextsClosed).extracting(FakeBulkImportContext::job).containsExactly(job);
        }
    }

    @Nested
    @DisplayName("Commit to state store")
    class CommitToStateStore {

        @Test
        void shouldCommitNewFilesAsynchronouslyWhenConfigured() throws Exception {
            // Given
            tableProperties.set(BULK_IMPORT_FILES_COMMIT_ASYNC, "true");
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
            Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
            List<FileReference> outputFiles = List.of(
                    defaultFileOnRootPartitionWithRows("file1.parquet", 100),
                    defaultFileOnRootPartitionWithRows("file2.parquet", 200));

            // When
            runJob(job, "test-run", "test-task", validationTime, driver(
                    successfulWithOutput(outputFiles), startAndFinishTime(startTime, finishTime)));

            // Then
            assertThat(allJobsReported())
                    .containsExactly(ingestJobStatus(job.getId(), jobRunOnTask("test-task",
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(startTime, 1),
                            ingestFinishedStatusUncommitted(finishTime, 2, new RowsProcessed(300, 300)))));
            assertThat(stateStore.getFileReferences()).isEmpty();
            assertThat(commitRequestQueue).containsExactly(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                    AddFilesTransaction.builder()
                            .jobId(job.getId()).taskId("test-task").jobRunId("test-run").writtenTime(finishTime)
                            .fileReferences(outputFiles)
                            .build()));
        }

        @Test
        void shouldNotRecordJobTrackerUpdateDetailsInTransactionLogForSynchronousCommit() throws Exception {
            // Given
            tableProperties.set(BULK_IMPORT_FILES_COMMIT_ASYNC, "false");
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
            Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
            List<FileReference> outputFiles = List.of(
                    defaultFileOnRootPartitionWithRows("test-output.parquet", 100));

            // When
            runJob(job, "test-run", "test-task", validationTime,
                    driver(successfulWithOutput(outputFiles), startAndFinishTime(startTime, finishTime)));

            // Then
            assertThat(transactionLogs.getLastFilesTransaction(tableProperties))
                    .isEqualTo(AddFilesTransaction.fromReferences(outputFiles));
        }
    }

    @Nested
    @DisplayName("Pre-split partition tree")
    class PreSplitPartitions {

        @Test
        void shouldPreSplitPartitionsWhenNotEnoughArePresent() throws Exception {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 1);
            tableProperties.setSchema(createSchemaWithKey("key", new IntType()));
            PartitionTree partitionsBefore = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            update(stateStore).initialise(partitionsBefore);
            setPartitionSketchData("root", List.of(
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 75))));
            PartitionTree partitionsAfter = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "P1", "P2", 50)
                    .buildTree();

            // And some output for the bulk import job
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
            Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(partitionsAfter);
            List<FileReference> outputFiles = List.of(
                    fileFactory.partitionFile("P1", 100),
                    fileFactory.partitionFile("P2", 100));

            // When
            runJob(job, "test-run", "test-task", validationTime, driver(
                    successfulWithOutput(outputFiles), startAndFinishTime(startTime, finishTime)));

            // Then
            assertThat(allJobsReported())
                    .containsExactly(ingestJobStatus(job.getId(), jobRunOnTask("test-task",
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(startTime, 1),
                            ingestFinishedStatus(summary(startTime, finishTime, 200, 200), 2))));
            assertThat(stateStore.getFileReferences()).isEqualTo(outputFiles);
            assertThat(stateStore.getAllPartitions())
                    .withRepresentation(PartitionsPrinter.representation(tableProperties))
                    .isEqualTo(partitionsAfter.getAllPartitions());
            assertThat(jobContextsCreated)
                    .extracting(FakeBulkImportContext::partitions)
                    .containsExactly(
                            partitionsBefore.getAllPartitions(),
                            partitionsAfter.getAllPartitions());
            assertThat(commitRequestQueue).isEmpty();
            assertThat(jobContextsClosed).extracting(FakeBulkImportContext::job).containsExactly(job);
        }

        @Test
        void shouldTrackFailedPartitionSplitting() throws Exception {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 5); // Not enough rows in sketch data
            tableProperties.setSchema(createSchemaWithKey("key", new IntType()));
            PartitionTree partitionsBefore = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            update(stateStore).initialise(partitionsBefore);
            setPartitionSketchData("root", List.of(
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 75))));
            BulkImportJob job = singleFileImportJob();
            Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
            Instant failureTime = Instant.parse("2023-04-06T12:41:01Z");
            var driver = driver(
                    failWithException(new IllegalStateException("Did not expect job to start")), failureTime(failureTime));

            // When / Then
            assertThatThrownBy(() -> runJob(job, "test-run", "test-task", validationTime, driver))
                    .isInstanceOf(InsufficientDataForPartitionSplittingException.class);
            assertThat(allJobsReported())
                    .containsExactly(ingestJobStatus(job.getId(), jobRunOnTask("test-task",
                            ingestAcceptedStatus(validationTime, 1),
                            failedStatus(failureTime, List.of("Required 2 minimum leaf partitions. " +
                                    "Unable to reach more than 1 leaf partitions based on the given data. " +
                                    "Either there are not enough unique values for the row key fields, or not enough data was provided.")))));
            assertThat(stateStore.getFileReferences()).isEmpty();
            assertThat(stateStore.getAllPartitions()).isEqualTo(partitionsBefore.getAllPartitions());
            assertThat(jobContextsCreated)
                    .extracting(FakeBulkImportContext::partitions)
                    .containsExactly(partitionsBefore.getAllPartitions());
            assertThat(jobContextsClosed).extracting(FakeBulkImportContext::job).containsExactly(job);
        }

    }

    private void runJob(
            BulkImportJob job, String jobRunId, String taskId, Instant validationTime,
            BulkImportJobDriver<FakeBulkImportContext> driver) throws Exception {
        tracker.jobValidated(job.toIngestJob().acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        driver.run(job, jobRunId, taskId);
    }

    private BulkImportJobDriver<FakeBulkImportContext> driver(
            BulkImportJobDriver.BulkImporter<FakeBulkImportContext> sessionRunner, Supplier<Instant> timeSupplier) {
        return driver(sessionRunner, stateStore, timeSupplier);
    }

    private BulkImportJobDriver<FakeBulkImportContext> driver(
            BulkImportJobDriver.BulkImporter<FakeBulkImportContext> sessionRunner, StateStore stateStore, Supplier<Instant> timeSupplier) {
        return new BulkImportJobDriver<>(
                FakeBulkImportContext.creator(jobContextsCreated, jobContextsClosed), dataSketcher(), sessionRunner,
                new FixedTablePropertiesProvider(tableProperties),
                FixedStateStoreProvider.singleTable(tableProperties, stateStore),
                tracker, commitRequestQueue::add, timeSupplier, supplyNumberedIdsWithPrefix("P"));
    }

    private void setPartitionSketchData(String partitionId, List<Row> rows) {
        Sketches sketches = Sketches.from(tableProperties.getSchema());
        rows.forEach(sketches::update);
        partitionIdToSketches.put(partitionId, sketches);
    }

    private BulkImportJobDriver.DataSketcher<FakeBulkImportContext> dataSketcher() {
        return context -> partitionIdToSketches;
    }

    private BulkImportJobDriver.BulkImporter<FakeBulkImportContext> successfulWithOutput(List<FileReference> outputFiles) {
        return context -> outputFiles;
    }

    private BulkImportJobDriver.BulkImporter<FakeBulkImportContext> failWithException(RuntimeException e) {
        return context -> {
            throw e;
        };
    }

    private Supplier<Instant> startAndFinishTime(Instant startTime, Instant finishTime) {
        return List.of(startTime, finishTime).iterator()::next;
    }

    private Supplier<Instant> startFinishAndFailureTime(Instant startTime, Instant finishTime, Instant failureTime) {
        return List.of(startTime, finishTime, failureTime).iterator()::next;
    }

    private Supplier<Instant> failureTime(Instant failureTime) {
        return List.of(failureTime).iterator()::next;
    }

    private BulkImportJob singleFileImportJob() {
        return BulkImportJob.builder()
                .id("test-job")
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .files(List.of("test.parquet")).build();
    }

    private List<IngestJobStatus> allJobsReported() {
        return tracker.getAllJobs(tableProperties.get(TABLE_ID));
    }
}
