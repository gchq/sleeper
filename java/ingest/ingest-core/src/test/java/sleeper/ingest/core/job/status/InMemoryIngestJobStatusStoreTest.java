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
package sleeper.ingest.core.job.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.update.IngestJobEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.ingest.core.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.filesWithReferences;
import static sleeper.ingest.core.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobFinishedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunOnTask;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunWhichFailed;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunWhichFinished;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunWhichStarted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.finishedIngestJob;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestAddedFilesStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestFinishedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestFinishedStatusUncommitted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.rejectedRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.startedIngestRun;

public class InMemoryIngestJobStatusStoreTest {

    private final InMemoryIngestJobStatusStore store = new InMemoryIngestJobStatusStore();
    private final TableStatus table = createTable("test-table");
    private final String tableId = table.getTableUniqueId();

    @Nested
    @DisplayName("Get all jobs")
    class GetAllJobs {
        @Test
        public void shouldReturnOneStartedJobWithNoFiles() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(0).build();

            store.jobStarted(job);
            assertThat(store.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job, ProcessRun.started(taskId, ingestStartedStatus(startTime, 0))));
        }

        @Test
        public void shouldReturnOneStartedJobWithFiles() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(2).build();

            store.jobStarted(job);
            assertThat(store.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job, ProcessRun.started(taskId, ingestStartedStatus(startTime, 2))));
        }

        @Test
        public void shouldReturnOneFinishedJobWithFiles() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            Instant finishTime = Instant.parse("2022-09-22T12:00:44.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(2).build();
            RecordsProcessedSummary summary = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime, finishTime);

            store.jobStarted(job);
            store.jobFinished(ingestJobFinishedEventBuilder(job, summary).taskId(taskId).numFilesWrittenByJob(1).build());
            assertThat(store.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job, ProcessRun.finished(taskId,
                            ingestStartedStatus(startTime, 2),
                            ingestFinishedStatus(summary, 1))));
        }

        @Test
        public void shouldRefuseJobFinishedWhenNotStarted() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            Instant finishTime = Instant.parse("2022-09-22T12:00:44.000Z");
            IngestJob job = createJobWithTableAndFiles("test-job", table, "test-file-1.parquet", "test-file-2.parquet");
            RecordsProcessedSummary summary = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime, finishTime);

            IngestJobFinishedEvent event = job.finishedEventBuilder(summary)
                    .taskId(taskId).numFilesWrittenByJob(1).build();
            assertThatThrownBy(() -> store.jobFinished(event))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        public void shouldReturnTwoRunsOnSameJob() {
            String taskId = "test-task";
            String jobId = "test-job";
            Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
            RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                    new RecordsProcessed(100L, 100L), startTime1, Duration.ofSeconds(15));
            RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime2, Duration.ofSeconds(30));
            IngestJobStartedEvent run1 = ingestJobStartedEventBuilder(jobId, startTime1).taskId(taskId).fileCount(2).build();
            IngestJobStartedEvent run2 = ingestJobStartedEventBuilder(jobId, startTime2).taskId(taskId).fileCount(2).build();
            store.jobStarted(run1);
            store.jobFinished(ingestJobFinishedEventBuilder(run1, summary1).taskId(taskId).numFilesWrittenByJob(1).build());
            store.jobStarted(run2);
            store.jobFinished(ingestJobFinishedEventBuilder(run2, summary2).taskId(taskId).numFilesWrittenByJob(2).build());

            assertThat(store.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(jobId,
                            ProcessRun.finished(taskId, ingestStartedStatus(startTime2, 2), ingestFinishedStatus(summary2, 2)),
                            ProcessRun.finished(taskId, ingestStartedStatus(startTime1, 2), ingestFinishedStatus(summary1, 1))));
        }

        @Test
        public void shouldReturnTwoJobs() {
            String taskId = "test-task";
            Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
            RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                    new RecordsProcessed(100L, 100L), startTime1, Duration.ofSeconds(15));
            RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime2, Duration.ofSeconds(30));
            IngestJobStartedEvent job1 = ingestJobStartedEventBuilder(startTime1).taskId(taskId).fileCount(1).build();
            IngestJobStartedEvent job2 = ingestJobStartedEventBuilder(startTime2).taskId(taskId).fileCount(2).build();

            store.jobStarted(job1);
            store.jobFinished(ingestJobFinishedEventBuilder(job1, summary1).taskId(taskId).numFilesWrittenByJob(3).build());
            store.jobStarted(job2);
            store.jobFinished(ingestJobFinishedEventBuilder(job2, summary2).taskId(taskId).numFilesWrittenByJob(4).build());

            assertThat(store.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job2, ProcessRun.finished(taskId, ingestStartedStatus(startTime2, 2), ingestFinishedStatus(summary2, 4))),
                    ingestJobStatus(job1, ProcessRun.finished(taskId, ingestStartedStatus(startTime1, 1), ingestFinishedStatus(summary1, 3))));
        }

        @Test
        public void shouldReturnJobsWithCorrectTable() {
            // Given
            String tableId1 = "test-table-1";
            String tableId2 = "test-table-2";
            String taskId = "test-task";
            Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
            RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                    new RecordsProcessed(100L, 100L), startTime1, Duration.ofSeconds(15));
            RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime2, Duration.ofSeconds(30));
            IngestJobStartedEvent job1 = ingestJobStartedEventBuilder(startTime1).taskId(taskId).tableId(tableId1).fileCount(1).build();
            IngestJobStartedEvent job2 = ingestJobStartedEventBuilder(startTime2).taskId(taskId).tableId(tableId2).fileCount(2).build();

            // When
            store.jobStarted(job1);
            store.jobFinished(ingestJobFinishedEventBuilder(job1, summary1).taskId(taskId).numFilesWrittenByJob(3).build());
            store.jobStarted(job2);
            store.jobFinished(ingestJobFinishedEventBuilder(job2, summary2).taskId(taskId).numFilesWrittenByJob(4).build());

            // Then
            assertThat(store.getAllJobs(tableId2)).containsExactly(
                    ingestJobStatus(job2, ProcessRun.finished(taskId, ingestStartedStatus(startTime2, 2), ingestFinishedStatus(summary2, 4))));
            assertThat(store.getAllJobs(tableId1)).containsExactly(
                    ingestJobStatus(job1, ProcessRun.finished(taskId, ingestStartedStatus(startTime1, 1), ingestFinishedStatus(summary1, 3))));
        }

        @Test
        public void shouldReturnNoJobsIfTableHasNoJobs() {
            assertThat(store.getAllJobs("table-with-no-jobs"))
                    .isEmpty();
        }

        @Test
        public void shouldReturnJobsWithSameIdOnDifferentTables() {
            TableStatus table1 = createTable("test-table-1");
            TableStatus table2 = createTable("test-table-2");
            String taskId = "test-task";
            IngestJob job1 = createJobWithTableAndFiles("test-job", table1, "test-file-1.parquet", "test-file-2.parquet");
            IngestJob job2 = createJobWithTableAndFiles("test-job", table2, "test-file-3.parquet");
            Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");

            // When
            store.jobStarted(job1.startedEventBuilder(startTime1).taskId(taskId).build());
            store.jobStarted(job2.startedEventBuilder(startTime2).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(table2.getTableUniqueId())).containsExactly(
                    ingestJobStatus(job2, startedIngestRun(job2, taskId, startTime2)));
            assertThat(store.getAllJobs(table1.getTableUniqueId())).containsExactly(
                    ingestJobStatus(job1, startedIngestRun(job1, taskId, startTime1)));
        }
    }

    @Nested
    @DisplayName("Get invalid jobs")
    class GetInvalidJobs {
        @Test
        void shouldGetInvalidJobsWithOneRejectedJob() {
            // Given
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobValidated(job.createRejectedEvent(validationTime, List.of("Test validation reason")));

            // Then
            assertThat(store.getInvalidJobs())
                    .containsExactly(ingestJobStatus(job, rejectedRun(job,
                            validationTime, "Test validation reason")));
        }

        @Test
        void shouldGetOneInvalidJobWithOneRejectedJobAndOneAcceptedJob() {
            // Given
            IngestJob job1 = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            IngestJob job2 = createJobWithTableAndFiles("test-job-2", table, "test-file-2.parquet");

            Instant validationTime1 = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant validationTime2 = Instant.parse("2022-09-22T12:02:10.000Z");

            // When
            store.jobValidated(job1.createRejectedEvent(validationTime1, List.of("Test validation reason")));
            store.jobValidated(job2.acceptedEventBuilder(validationTime2).build());

            // Then
            assertThat(store.getInvalidJobs())
                    .containsExactly(ingestJobStatus(job1, rejectedRun(job1,
                            validationTime1, "Test validation reason")));
        }

        @Test
        void shouldGetInvalidJobsAcrossMultipleTables() {
            TableStatus table1 = createTable("test-table-1");
            TableStatus table2 = createTable("test-table-2");
            IngestJob job1 = createJobWithTableAndFiles("test-job-1", table1, "test-file-1.parquet");
            IngestJob job2 = createJobWithTableAndFiles("test-job-2", table2, "test-file-2.parquet");
            Instant validationTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant validationTime2 = Instant.parse("2022-09-22T12:00:31.000Z");

            // When
            store.jobValidated(job1.createRejectedEvent(validationTime1, List.of("Test reason 1")));
            store.jobValidated(job2.createRejectedEvent(validationTime2, List.of("Test reason 2")));

            // Then
            assertThat(store.getInvalidJobs()).containsExactly(
                    ingestJobStatus(job2, rejectedRun(job2, validationTime2, "Test reason 2")),
                    ingestJobStatus(job1, rejectedRun(job1, validationTime1, "Test reason 1")));
        }

        @Test
        void shouldNotGetJobThatWasRejectedThenAccepted() {
            // Given
            IngestJob job1 = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");

            Instant validationTime1 = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant validationTime2 = Instant.parse("2022-09-22T12:02:10.000Z");

            // When
            store.jobValidated(job1.createRejectedEvent(validationTime1, List.of("Test validation reason")));
            store.jobValidated(job1.acceptedEventBuilder(validationTime2).build());

            // Then
            assertThat(store.getInvalidJobs()).isEmpty();
        }

        @Test
        void shouldGetInvalidJobWithNoTable() {
            // Given
            IngestJob job = IngestJob.builder().id("test-job").build();
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobValidated(job.createRejectedEvent(validationTime, List.of("Test validation reason")));

            // Then
            assertThat(store.getInvalidJobs()).containsExactly(
                    ingestJobStatus(job, rejectedRun(job, validationTime, "Test validation reason")));
        }
    }

    @Nested
    @DisplayName("Report validation of job")
    class ReportValidationStatus {
        @Test
        void shouldReportUnstartedJobWithNoValidationFailures() {
            // Given
            String taskId = "some-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobValidated(job.acceptedEventBuilder(validationTime).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, acceptedRunOnTask(job, taskId, validationTime)));
        }

        @Test
        void shouldReportStartedJobWithNoValidationFailures() {
            // Given
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

            // When
            store.jobValidated(job.acceptedEventBuilder(validationTime).taskId(taskId).build());
            store.jobStarted(job.startedAfterValidationEventBuilder(startTime).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, acceptedRunWhichStarted(job, taskId,
                            validationTime, startTime)));
        }

        @Test
        void shouldReportJobWithOneValidationFailure() {
            // Given
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobValidated(job.createRejectedEvent(validationTime, List.of("Test validation reason")));

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, rejectedRun(job,
                            validationTime, "Test validation reason")));
        }

        @Test
        void shouldReportJobWithMultipleValidationFailures() {
            // Given
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobValidated(job.createRejectedEvent(validationTime,
                    List.of("Test validation reason 1", "Test validation reason 2")));

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, rejectedRun(job, validationTime,
                            List.of("Test validation reason 1", "Test validation reason 2"))));
        }
    }

    @Nested
    @DisplayName("Correlate runs by ID")
    class CorrelateRunsById {
        @Test
        void shouldReportAcceptedJob() {
            // Given
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId("test-run").build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, acceptedRun(job, validationTime)));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run");
        }

        @Test
        void shouldReportStartedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

            // When
            store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
            store.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, acceptedRunWhichStarted(job, taskId,
                            validationTime, startTime)));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run");
        }

        @Test
        void shouldReportFinishedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);

            // When
            store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
            store.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobFinished(job.finishedEventBuilder(summary).jobRunId(jobRunId).taskId(taskId).numFilesWrittenByJob(2).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, acceptedRunWhichFinished(job, taskId,
                            validationTime, summary, 2)));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run", "test-run");
        }

        @Test
        void shouldReportUnvalidatedFinishedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);

            // When
            store.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobFinished(job.finishedEventBuilder(summary).jobRunId(jobRunId).taskId(taskId).numFilesWrittenByJob(2).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(finishedIngestJob(job, taskId, summary, 2));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run");
        }

        @Test
        void shouldReportFailedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            ProcessRunTime runTime = new ProcessRunTime(startTime, Duration.ofMinutes(10));
            List<String> failureReasons = List.of("Something went wrong");

            // When
            store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
            store.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobFailed(job.failedEventBuilder(runTime).jobRunId(jobRunId).taskId(taskId).failureReasons(failureReasons).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, acceptedRunWhichFailed(job, taskId,
                            validationTime, runTime, failureReasons)));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run", "test-run");
        }
    }

    @Nested
    @DisplayName("Report files committed by job")
    class ReportFileCommits {

        @Test
        void shouldReportJobInProgressWithOneCommit() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            Instant writtenTime = Instant.parse("2024-06-19T14:50:30.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(fileFactory.rootFile("file.parquet", 123)));

            // When
            store.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobAddedFiles(job.addedFilesEventBuilder(writtenTime).files(filesAdded).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 1))
                            .build()));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run");
        }

        @Test
        void shouldReportJobAddedOneFileWithTwoReferences() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            Instant writtenTime = Instant.parse("2024-06-19T14:50:30.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key", new LongType()))
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(
                    fileFactory.partitionFile("L", "file.parquet", 50),
                    fileFactory.partitionFile("R", "file.parquet", 50)));

            // When
            store.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobAddedFiles(job.addedFilesEventBuilder(writtenTime).files(filesAdded).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 1))
                            .build()));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run");
        }

        @Test
        void shouldReportJobAddedTwoFiles() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            Instant writtenTime = Instant.parse("2024-06-19T14:50:30.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(
                    fileFactory.rootFile("file1.parquet", 123),
                    fileFactory.rootFile("file2.parquet", 456)));

            // When
            store.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobAddedFiles(job.addedFilesEventBuilder(writtenTime).files(filesAdded).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 2))
                            .build()));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run");
        }

        @Test
        void shouldReportJobFinishedButUncommitted() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(
                    fileFactory.rootFile("file1.parquet", 123)));
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(1), 123, 123);

            // When
            store.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
            store.jobFinished(job.finishedEventBuilder(summary)
                    .jobRunId(jobRunId).taskId(taskId)
                    .filesWrittenByJob(filesAdded).committedBySeparateFileUpdates(true)
                    .build());

            // Then
            assertThat(store.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestFinishedStatusUncommitted(summary, 1))
                            .build()));
            assertThat(store.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly("test-run", "test-run");
        }
    }

    private TableStatus createTable(String tableName) {
        return TableStatusTestHelper.uniqueIdAndName(new TableIdGenerator().generateString(), tableName);
    }

    private IngestJobStartedEvent.Builder ingestJobStartedEventBuilder(Instant startTime) {
        return IngestJobEventTestData.ingestJobStartedEventBuilder(startTime).tableId(tableId);
    }

    private IngestJobStartedEvent.Builder ingestJobStartedEventBuilder(String jobId, Instant startTime) {
        return ingestJobStartedEventBuilder(startTime).jobId(jobId);
    }

    private IngestJobStatus ingestJobStatus(IngestJob job, ProcessRun... runs) {
        return IngestJobStatusFromJobTestData.ingestJobStatus(job, runs);
    }

    private IngestJobStatus ingestJobStatus(IngestJobEvent job, ProcessRun... runs) {
        return IngestJobStatusTestHelper.ingestJobStatus(job, runs);
    }

    private IngestJobStatus ingestJobStatus(String jobId, ProcessRun... runs) {
        return IngestJobStatusTestHelper.ingestJobStatus(jobId, runs);
    }
}
