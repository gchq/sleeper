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

import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.StateStoreTestHelper;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.defaultFileOnRootPartitionWithRecords;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.acceptedRunWhichFailed;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestAcceptedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestFinishedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestFinishedStatusUncommitted;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.validatedIngestStartedStatus;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

class BulkImportJobDriverTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition(schema);
    private final IngestJobStatusStore statusStore = new InMemoryIngestJobStatusStore();
    private final List<IngestAddFilesCommitRequest> commitRequestQueue = new ArrayList<>();

    @Test
    void shouldReportJobFinished() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));

        // When
        runJob(job, "test-run", "test-task", validationTime,
                driver(successfulWithOutput(outputFiles), startAndFinishTime(startTime, finishTime)));

        // Then
        IngestJob ingestJob = job.toIngestJob();
        assertThat(allJobsReported())
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId("test-task")
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatus(ingestJob,
                                summary(startTime, finishTime, 100, 100), 1))
                        .build()));
        assertThat(stateStore.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .isEqualTo(outputFiles);
        assertThat(commitRequestQueue).isEmpty();
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
        BulkImportJobDriver driver = driver(
                failWithException(jobFailure), startAndFinishTime(startTime, finishTime));
        assertThatThrownBy(() -> runJob(job, "test-run", "test-task", validationTime, driver))
                .isSameAs(jobFailure);

        // Then
        assertThat(allJobsReported())
                .containsExactly(jobStatus(job.toIngestJob(), acceptedRunWhichFailed(
                        job.toIngestJob(), "test-task", validationTime,
                        new ProcessRunTime(startTime, finishTime),
                        List.of("Failed running job", "Some cause", "Root cause"))));
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(commitRequestQueue).isEmpty();
    }

    @Test
    void shouldReportJobFinishedWithNoRecordsWhenStateStoreUpdateFailed() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        StateStoreException jobFailure = new StateStoreException("Failed updating files");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));
        StateStore stateStore = mock(StateStore.class);
        doThrow(jobFailure).when(stateStore).addFiles(outputFiles);

        // When
        BulkImportJobDriver driver = driver(
                successfulWithOutput(outputFiles), stateStore, startAndFinishTime(startTime, finishTime));
        assertThatThrownBy(() -> runJob(job, "test-run", "test-task", validationTime, driver))
                .isInstanceOf(RuntimeException.class).hasCauseReference(jobFailure);

        // Then
        assertThat(allJobsReported())
                .containsExactly(jobStatus(job.toIngestJob(), acceptedRunWhichFailed(
                        job.toIngestJob(), "test-task", validationTime,
                        new ProcessRunTime(startTime, finishTime),
                        List.of("Failed updating files"))));
        verify(stateStore).addFiles(outputFiles);
        verifyNoMoreInteractions(stateStore);
        assertThat(commitRequestQueue).isEmpty();
    }

    @Test
    void shouldReportJobFinishedWithNoRecordsWhenStateStoreUpdateHadUnexpectedFailure() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        RuntimeException jobFailure = new RuntimeException("Failed updating files");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));
        StateStore stateStore = mock(StateStore.class);
        doThrow(jobFailure).when(stateStore).addFiles(outputFiles);

        // When
        BulkImportJobDriver driver = driver(
                successfulWithOutput(outputFiles), stateStore, startAndFinishTime(startTime, finishTime));
        assertThatThrownBy(() -> runJob(job, "test-run", "test-task", validationTime, driver))
                .isInstanceOf(RuntimeException.class).hasCauseReference(jobFailure);

        // Then
        assertThat(allJobsReported())
                .containsExactly(jobStatus(job.toIngestJob(), acceptedRunWhichFailed(
                        job.toIngestJob(), "test-task", validationTime,
                        new ProcessRunTime(startTime, finishTime),
                        List.of("Failed updating files"))));
        verify(stateStore).addFiles(outputFiles);
        verifyNoMoreInteractions(stateStore);
        assertThat(commitRequestQueue).isEmpty();
    }

    @Test
    void shouldCommitNewFilesAsynchronouslyWhenConfigured() throws Exception {
        // Given
        tableProperties.set(BULK_IMPORT_FILES_COMMIT_ASYNC, "true");
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant writtenTime = Instant.parse("2023-04-06T12:41:00Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("file1.parquet", 100),
                defaultFileOnRootPartitionWithRecords("file2.parquet", 200));

        // When
        runJob(job, "test-run", "test-task", validationTime, driver(
                successfulWithOutput(outputFiles), startWrittenAndFinishTime(startTime, writtenTime, finishTime)));

        // Then
        IngestJob ingestJob = job.toIngestJob();
        assertThat(allJobsReported())
                .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                        .taskId("test-task")
                        .startedStatus(ingestAcceptedStatus(ingestJob, validationTime))
                        .statusUpdate(validatedIngestStartedStatus(ingestJob, startTime))
                        .finishedStatus(ingestFinishedStatusUncommitted(ingestJob,
                                summary(startTime, finishTime, 300, 300), 2))
                        .build()));
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(commitRequestQueue).containsExactly(IngestAddFilesCommitRequest.builder()
                .ingestJob(ingestJob)
                .fileReferences(outputFiles)
                .jobRunId("test-run").taskId("test-task")
                .writtenTime(writtenTime)
                .build());
    }

    private void runJob(
            BulkImportJob job, String jobRunId, String taskId, Instant validationTime,
            BulkImportJobDriver driver) throws Exception {
        statusStore.jobValidated(ingestJobAccepted(job.toIngestJob(), validationTime).jobRunId(jobRunId).build());
        driver.run(job, jobRunId, taskId);
    }

    private BulkImportJobDriver driver(
            BulkImportJobDriver.SessionRunner sessionRunner, Supplier<Instant> timeSupplier) {
        return driver(sessionRunner, stateStore, timeSupplier);
    }

    private BulkImportJobDriver driver(
            BulkImportJobDriver.SessionRunner sessionRunner, StateStore stateStore, Supplier<Instant> timeSupplier) {
        return new BulkImportJobDriver(sessionRunner,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                statusStore, commitRequestQueue::add, timeSupplier);
    }

    private BulkImportJobDriver.SessionRunner successfulWithOutput(List<FileReference> outputFiles) {
        BulkImportJobOutput output = new BulkImportJobOutput(outputFiles, () -> {
        });
        return job -> output;
    }

    private BulkImportJobDriver.SessionRunner failWithException(RuntimeException e) {
        return job -> {
            throw e;
        };
    }

    private Supplier<Instant> startAndFinishTime(Instant startTime, Instant finishTime) {
        return List.of(startTime, finishTime).iterator()::next;
    }

    private Supplier<Instant> startWrittenAndFinishTime(Instant startTime, Instant writtenTime, Instant finishTime) {
        return List.of(startTime, writtenTime, finishTime).iterator()::next;
    }

    private BulkImportJob singleFileImportJob() {
        return BulkImportJob.builder()
                .id("test-job")
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .files(List.of("test.parquet")).build();
    }

    private List<IngestJobStatus> allJobsReported() {
        return statusStore.getAllJobs(tableProperties.get(TABLE_ID));
    }
}
