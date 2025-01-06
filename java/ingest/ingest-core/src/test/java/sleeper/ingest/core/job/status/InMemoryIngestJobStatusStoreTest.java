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
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.update.IngestJobEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.record.process.ProcessRunTestData.finishedRun;
import static sleeper.core.record.process.ProcessRunTestData.startedRun;
import static sleeper.core.record.process.ProcessRunTestData.unfinishedRun;
import static sleeper.core.record.process.ProcessRunTestData.validatedFinishedRun;
import static sleeper.core.record.process.ProcessRunTestData.validationRun;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.failedStatus;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.filesWithReferences;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobAcceptedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobAddedFilesEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobFailedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobFinishedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobRejectedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobStartedAfterValidationEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobStartedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobEventTestData.ingestJobValidatedEventBuilder;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.ingestAcceptedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.ingestFinishedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.ingestFinishedStatusUncommitted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.ingestRejectedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestData.validatedIngestStartedStatus;

public class InMemoryIngestJobStatusStoreTest {

    private final InMemoryIngestJobStatusStore tracker = new InMemoryIngestJobStatusStore();
    private final String tableId = IngestJobEventTestData.DEFAULT_TABLE_ID;

    @Nested
    @DisplayName("Get all jobs")
    class GetAllJobs {
        @Test
        public void shouldReturnOneStartedJobWithNoFiles() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(0).build();

            tracker.jobStarted(job);
            assertThat(tracker.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job, startedRun(taskId, ingestStartedStatus(startTime, 0))));
        }

        @Test
        public void shouldReturnOneStartedJobWithFiles() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(2).build();

            tracker.jobStarted(job);
            assertThat(tracker.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job, startedRun(taskId, ingestStartedStatus(startTime, 2))));
        }

        @Test
        public void shouldReturnOneFinishedJobWithFiles() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            Instant finishTime = Instant.parse("2022-09-22T12:00:44.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(2).build();
            RecordsProcessedSummary summary = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime, finishTime);

            tracker.jobStarted(job);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job, summary).taskId(taskId).numFilesWrittenByJob(1).build());
            assertThat(tracker.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job, finishedRun(taskId,
                            ingestStartedStatus(startTime, 2),
                            ingestFinishedStatus(summary, 1))));
        }

        @Test
        public void shouldRefuseJobFinishedWhenNotStarted() {
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
            Instant finishTime = Instant.parse("2022-09-22T12:00:44.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).taskId(taskId).fileCount(2).build();
            RecordsProcessedSummary summary = new RecordsProcessedSummary(
                    new RecordsProcessed(200L, 200L), startTime, finishTime);

            IngestJobFinishedEvent event = ingestJobFinishedEventBuilder(job, summary)
                    .taskId(taskId).numFilesWrittenByJob(1).build();
            assertThatThrownBy(() -> tracker.jobFinished(event))
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
            IngestJobStartedEvent run1 = ingestJobStartedEventBuilder(startTime1).jobId(jobId).taskId(taskId).fileCount(2).build();
            IngestJobStartedEvent run2 = ingestJobStartedEventBuilder(startTime2).jobId(jobId).taskId(taskId).fileCount(2).build();
            tracker.jobStarted(run1);
            tracker.jobFinished(ingestJobFinishedEventBuilder(run1, summary1).taskId(taskId).numFilesWrittenByJob(1).build());
            tracker.jobStarted(run2);
            tracker.jobFinished(ingestJobFinishedEventBuilder(run2, summary2).taskId(taskId).numFilesWrittenByJob(2).build());

            assertThat(tracker.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(jobId,
                            finishedRun(taskId, ingestStartedStatus(startTime2, 2), ingestFinishedStatus(summary2, 2)),
                            finishedRun(taskId, ingestStartedStatus(startTime1, 2), ingestFinishedStatus(summary1, 1))));
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

            tracker.jobStarted(job1);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job1, summary1).taskId(taskId).numFilesWrittenByJob(3).build());
            tracker.jobStarted(job2);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job2, summary2).taskId(taskId).numFilesWrittenByJob(4).build());

            assertThat(tracker.getAllJobs(tableId)).containsExactly(
                    ingestJobStatus(job2, finishedRun(taskId, ingestStartedStatus(startTime2, 2), ingestFinishedStatus(summary2, 4))),
                    ingestJobStatus(job1, finishedRun(taskId, ingestStartedStatus(startTime1, 1), ingestFinishedStatus(summary1, 3))));
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
            tracker.jobStarted(job1);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job1, summary1).taskId(taskId).numFilesWrittenByJob(3).build());
            tracker.jobStarted(job2);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job2, summary2).taskId(taskId).numFilesWrittenByJob(4).build());

            // Then
            assertThat(tracker.getAllJobs(tableId2)).containsExactly(
                    ingestJobStatus(job2, finishedRun(taskId, ingestStartedStatus(startTime2, 2), ingestFinishedStatus(summary2, 4))));
            assertThat(tracker.getAllJobs(tableId1)).containsExactly(
                    ingestJobStatus(job1, finishedRun(taskId, ingestStartedStatus(startTime1, 1), ingestFinishedStatus(summary1, 3))));
        }

        @Test
        public void shouldReturnNoJobsIfTableHasNoJobs() {
            assertThat(tracker.getAllJobs("table-with-no-jobs"))
                    .isEmpty();
        }

        @Test
        public void shouldReturnJobsWithSameIdOnDifferentTables() {
            String tableId1 = "test-table-1";
            String tableId2 = "test-table-2";
            String jobId = "reused-job-id";
            String taskId = "test-task";
            Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
            IngestJobStartedEvent job1 = ingestJobStartedEventBuilder(startTime1).jobId(jobId).tableId(tableId1).taskId(taskId).fileCount(2).build();
            IngestJobStartedEvent job2 = ingestJobStartedEventBuilder(startTime2).jobId(jobId).tableId(tableId2).taskId(taskId).fileCount(1).build();

            // When
            tracker.jobStarted(job1);
            tracker.jobStarted(job2);

            // Then
            assertThat(tracker.getAllJobs(tableId2)).containsExactly(
                    ingestJobStatus(jobId, startedRun(taskId, ingestStartedStatus(startTime2, 1))));
            assertThat(tracker.getAllJobs(tableId1)).containsExactly(
                    ingestJobStatus(jobId, startedRun(taskId, ingestStartedStatus(startTime1, 2))));
        }
    }

    @Nested
    @DisplayName("Get invalid jobs")
    class GetInvalidJobs {
        @Test
        void shouldGetInvalidJobsWithOneRejectedJob() {
            // Given
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            IngestJobValidatedEvent job = ingestJobRejectedEventBuilder(validationTime, List.of("Test validation reason")).jsonMessage("invalid-json").build();

            // When
            tracker.jobValidated(job);

            // Then
            assertThat(tracker.getInvalidJobs())
                    .containsExactly(ingestJobStatus(job,
                            validationRun(ingestRejectedStatus(validationTime, "invalid-json", List.of("Test validation reason"), 0))));
        }

        @Test
        void shouldGetOneInvalidJobWithOneRejectedJobAndOneAcceptedJob() {
            // Given
            Instant validationTime1 = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant validationTime2 = Instant.parse("2022-09-22T12:02:10.000Z");
            IngestJobValidatedEvent job1 = ingestJobRejectedEventBuilder(validationTime1, List.of("Test validation reason")).jsonMessage("invalid-json").build();
            IngestJobValidatedEvent job2 = ingestJobAcceptedEventBuilder(validationTime2).fileCount(3).build();

            // When
            tracker.jobValidated(job1);
            tracker.jobValidated(job2);

            // Then
            assertThat(tracker.getInvalidJobs())
                    .containsExactly(ingestJobStatus(job1,
                            validationRun(ingestRejectedStatus(validationTime1, "invalid-json", List.of("Test validation reason"), 0))));
        }

        @Test
        void shouldGetInvalidJobsAcrossMultipleTables() {
            String tableId1 = "test-table-1";
            String tableId2 = "test-table-2";
            Instant validationTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
            Instant validationTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
            IngestJobValidatedEvent job1 = ingestJobValidatedEventBuilder(validationTime1).tableId(tableId1).reasons(List.of("Test validation reason")).fileCount(1).build();
            IngestJobValidatedEvent job2 = ingestJobValidatedEventBuilder(validationTime2).tableId(tableId2).reasons(List.of("Other validation reason")).fileCount(2).build();

            // When
            tracker.jobValidated(job1);
            tracker.jobValidated(job2);

            // Then
            assertThat(tracker.getInvalidJobs()).containsExactly(
                    ingestJobStatus(job2, validationRun(ingestRejectedStatus(validationTime2, List.of("Other validation reason"), 2))),
                    ingestJobStatus(job1, validationRun(ingestRejectedStatus(validationTime1, List.of("Test validation reason"), 1))));
        }

        @Test
        void shouldNotGetJobThatWasRejectedThenAccepted() {
            // Given
            String jobId = "test-job-1";
            Instant validationTime1 = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant validationTime2 = Instant.parse("2022-09-22T12:02:10.000Z");

            // When
            tracker.jobValidated(ingestJobValidatedEventBuilder(validationTime1).jobId(jobId).reasons(List.of("Reason")).build());
            tracker.jobValidated(ingestJobValidatedEventBuilder(validationTime2).jobId(jobId).reasons(List.of()).build());

            // Then
            assertThat(tracker.getInvalidJobs()).isEmpty();
        }

        @Test
        void shouldGetInvalidJobWithNoTable() {
            // Given
            String jobId = "test-job";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            tracker.jobValidated(ingestJobValidatedEventBuilder(validationTime).tableId(null).jobId(jobId).reasons(List.of("Reason")).fileCount(2).build());

            // Then
            assertThat(tracker.getInvalidJobs()).containsExactly(
                    ingestJobStatus(jobId, validationRun(ingestRejectedStatus(validationTime, List.of("Reason"), 2))));
        }
    }

    @Nested
    @DisplayName("Report validation of job")
    class ReportValidationStatus {
        @Test
        void shouldReportUnstartedJobWithNoValidationFailures() {
            // Given
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            IngestJobValidatedEvent job = ingestJobAcceptedEventBuilder(validationTime).fileCount(1).jobRunId("test-run").build();

            // When
            tracker.jobValidated(job);

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, validationRun(ingestAcceptedStatus(validationTime, 1))));
        }

        @Test
        void shouldReportStartedJobWithNoValidationFailures() {
            // Given
            String taskId = "test-task";
            String jobRunId = "test-run";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            IngestJobValidatedEvent job = ingestJobAcceptedEventBuilder(validationTime).fileCount(1).jobRunId(jobRunId).build();

            // When
            tracker.jobValidated(job);
            tracker.jobStarted(ingestJobStartedAfterValidationEventBuilder(job, startTime).taskId(taskId).jobRunId(jobRunId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, unfinishedRun(taskId,
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(1, startTime))));
        }

        @Test
        void shouldReportJobWithOneValidationFailure() {
            // Given
            String jobId = "test-job-1";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            tracker.jobValidated(ingestJobRejectedEventBuilder(validationTime, List.of("Test validation reason"))
                    .jobId(jobId).jsonMessage("test-json").fileCount(1).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(jobId, validationRun(
                            ingestRejectedStatus(validationTime, "test-json", List.of("Test validation reason"), 1))));
        }

        @Test
        void shouldReportJobWithMultipleValidationFailures() {
            // Given
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            tracker.jobValidated(ingestJobRejectedEventBuilder(validationTime, List.of("Test validation reason 1", "Test validation reason 2"))
                    .jobId("test-job-1").jsonMessage("test-json").fileCount(1).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus("test-job-1", validationRun(
                            ingestRejectedStatus(validationTime, "test-json", List.of("Test validation reason 1", "Test validation reason 2"), 1))));
        }
    }

    @Nested
    @DisplayName("Correlate runs by ID")
    class CorrelateRunsById {

        @Test
        void shouldReportStartedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            IngestJobValidatedEvent job = ingestJobAcceptedEventBuilder(validationTime).fileCount(1)
                    .jobId("test-job-1").jobRunId(jobRunId).build();

            // When
            tracker.jobValidated(job);
            tracker.jobStarted(ingestJobStartedAfterValidationEventBuilder(job, startTime).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, unfinishedRun(taskId,
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(1, startTime))));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId);
        }

        @Test
        void shouldReportFinishedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);
            IngestJobValidatedEvent job = ingestJobAcceptedEventBuilder(validationTime).fileCount(1)
                    .jobId("test-job-1").jobRunId(jobRunId).build();

            // When
            tracker.jobValidated(job);
            tracker.jobStarted(ingestJobStartedAfterValidationEventBuilder(job, startTime).jobRunId(jobRunId).taskId(taskId).build());
            tracker.jobFinished(ingestJobFinishedEventBuilder(job, summary).jobRunId(jobRunId).taskId(taskId).numFilesWrittenByJob(2).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, validatedFinishedRun(taskId,
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(1, startTime),
                            ingestFinishedStatus(summary, 2))));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId, jobRunId);
        }

        @Test
        void shouldReportUnvalidatedFinishedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).fileCount(1)
                    .jobId("test-job-1").jobRunId(jobRunId).taskId(taskId).build();

            // When
            tracker.jobStarted(job);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job, summary).jobRunId(jobRunId).taskId(taskId).numFilesWrittenByJob(2).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, finishedRun(taskId,
                            ingestStartedStatus(startTime, 1),
                            ingestFinishedStatus(summary, 2))));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId);
        }

        @Test
        void shouldReportFailedJob() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            ProcessRunTime runTime = new ProcessRunTime(startTime, Duration.ofMinutes(10));
            List<String> failureReasons = List.of("Something went wrong");
            IngestJobValidatedEvent job = ingestJobAcceptedEventBuilder(validationTime).fileCount(1)
                    .jobId("test-job-1").jobRunId(jobRunId).build();

            // When
            tracker.jobValidated(job);
            tracker.jobStarted(ingestJobStartedAfterValidationEventBuilder(job, startTime).jobRunId(jobRunId).taskId(taskId).build());
            tracker.jobFailed(ingestJobFailedEventBuilder(job, runTime, failureReasons).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, validatedFinishedRun(taskId,
                            ingestAcceptedStatus(validationTime, 1),
                            validatedIngestStartedStatus(1, startTime),
                            failedStatus(runTime, failureReasons))));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId, jobRunId);
        }

        @Test
        void shouldFailToReportOnRunWhenJobRunIdIsNotSetAtValidationAndTaskIdIsOnlySetWhenStarted() {
            // A bulk import job is validated in the bulk import job starter lambda. That lambda doesn't have a task ID,
            // and you can imagine if it did it would be different to any task ID in the Spark cluster.
            // For standard ingest, the task ID is used to correlate events on the same run of a job. For bulk import,
            // we create a job run ID instead.
            // Validation events are currently always associated with a specific run of the job. If a job is retried it
            // will be revalidated.

            // Given
            String taskId = "test-task";
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
            IngestJobValidatedEvent job = ingestJobAcceptedEventBuilder(validationTime).fileCount(1).build();

            // When
            tracker.jobValidated(job);
            tracker.jobStarted(ingestJobStartedAfterValidationEventBuilder(job, startTime).taskId(taskId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, validationRun(
                            ingestAcceptedStatus(validationTime, 1))));
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
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).fileCount(2).jobRunId(jobRunId).taskId(taskId).build();
            Instant writtenTime = Instant.parse("2024-06-19T14:50:30.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(fileFactory.rootFile("file.parquet", 123)));

            // When
            tracker.jobStarted(job);
            tracker.jobAddedFiles(ingestJobAddedFilesEventBuilder(job, writtenTime).files(filesAdded).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime, 2))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 1))
                            .build()));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId);
        }

        @Test
        void shouldReportJobAddedOneFileWithTwoReferences() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).fileCount(1).jobRunId(jobRunId).taskId(taskId).build();
            Instant writtenTime = Instant.parse("2024-06-19T14:50:30.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key", new LongType()))
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123L)
                    .buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(
                    fileFactory.partitionFile("L", "file.parquet", 50),
                    fileFactory.partitionFile("R", "file.parquet", 50)));

            // When
            tracker.jobStarted(job);
            tracker.jobAddedFiles(ingestJobAddedFilesEventBuilder(job, writtenTime).files(filesAdded).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 1))
                            .build()));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId);
        }

        @Test
        void shouldReportJobAddedTwoFiles() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).fileCount(1).jobRunId(jobRunId).taskId(taskId).build();
            Instant writtenTime = Instant.parse("2024-06-19T14:50:30.000Z");
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(
                    fileFactory.rootFile("file1.parquet", 123),
                    fileFactory.rootFile("file2.parquet", 456)));

            // When
            tracker.jobStarted(job);
            tracker.jobAddedFiles(ingestJobAddedFilesEventBuilder(job, writtenTime).files(filesAdded).jobRunId(jobRunId).taskId(taskId).build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 2))
                            .build()));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId);
        }

        @Test
        void shouldReportJobFinishedButUncommitted() {
            // Given
            String jobRunId = "test-run";
            String taskId = "test-task";
            Instant startTime = Instant.parse("2024-06-19T14:50:00.000Z");
            IngestJobStartedEvent job = ingestJobStartedEventBuilder(startTime).fileCount(1).jobRunId(jobRunId).taskId(taskId).build();
            FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
            List<AllReferencesToAFile> filesAdded = filesWithReferences(List.of(
                    fileFactory.rootFile("file1.parquet", 123)));
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(1), 123, 123);

            // When
            tracker.jobStarted(job);
            tracker.jobFinished(ingestJobFinishedEventBuilder(job, summary)
                    .jobRunId(jobRunId).taskId(taskId)
                    .filesWrittenByJob(filesAdded).committedBySeparateFileUpdates(true)
                    .build());

            // Then
            assertThat(tracker.getAllJobs(tableId))
                    .containsExactly(ingestJobStatus(job, ProcessRun.builder()
                            .taskId(taskId)
                            .startedStatus(ingestStartedStatus(startTime))
                            .statusUpdate(ingestFinishedStatusUncommitted(summary, 1))
                            .build()));
            assertThat(tracker.streamTableRecords(tableId))
                    .extracting(ProcessStatusUpdateRecord::getJobRunId)
                    .containsExactly(jobRunId, jobRunId);
        }
    }

    private TableStatus createTable(String tableName) {
        return TableStatusTestHelper.uniqueIdAndName(IngestJobEventTestData.DEFAULT_TABLE_ID, tableName);
    }

    private IngestJobStatus ingestJobStatus(IngestJobEvent job, ProcessRun... runs) {
        return IngestJobStatusTestData.ingestJobStatus(job, runs);
    }

    private IngestJobStatus ingestJobStatus(String jobId, ProcessRun... runs) {
        return IngestJobStatusTestData.ingestJobStatus(jobId, runs);
    }
}
