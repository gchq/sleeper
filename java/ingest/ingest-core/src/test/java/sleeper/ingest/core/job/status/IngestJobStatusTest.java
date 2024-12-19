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

import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.ingest.core.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJob;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forRunOnNoTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forRunOnTask;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.withExpiry;
import static sleeper.core.tracker.ingest.job.IngestJobStatusType.ACCEPTED;
import static sleeper.core.tracker.ingest.job.IngestJobStatusType.FAILED;
import static sleeper.core.tracker.ingest.job.IngestJobStatusType.FINISHED;
import static sleeper.core.tracker.ingest.job.IngestJobStatusType.IN_PROGRESS;
import static sleeper.core.tracker.ingest.job.IngestJobStatusType.UNCOMMITTED;
import static sleeper.ingest.core.job.IngestJobTestData.createJobInDefaultTable;
import static sleeper.ingest.core.job.status.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.failedIngestRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.finishedIngestRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.finishedIngestRunUncommitted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatusListFrom;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.rejectedRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.singleJobStatusFrom;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.startedIngestRun;

public class IngestJobStatusTest {
    private final IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");

    @Nested
    @DisplayName("Report when a job is unfinished")
    class ReportUnfinished {
        @Test
        public void shouldReportIngestJobStarted() {
            // Given
            Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");

            // When
            IngestJobStatus status = ingestJobStatus(job, startedIngestRun(job, "test-task", startTime));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(true);
        }

        @Test
        public void shouldReportIngestJobFinished() {
            // Given
            Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
            Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");

            // When
            IngestJobStatus status = ingestJobStatus(job,
                    finishedIngestRun(job, "test-task", summary(startTime, finishTime)));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(false);
        }

        @Test
        public void shouldReportIngestJobUnstartedWhenAccepted() {
            // Given
            Instant validationTime = Instant.parse("2022-09-22T13:33:10.001Z");

            // When
            IngestJobStatus status = ingestJobStatus(job, acceptedRun(job, validationTime));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(true);
        }

        @Test
        public void shouldReportIngestJobNotInProgressWhenRejected() {
            // Given
            Instant validationTime = Instant.parse("2022-09-22T13:33:10.001Z");

            // When
            IngestJobStatus status = ingestJobStatus(job, rejectedRun(job, validationTime));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(false);
        }

        @Test
        public void shouldReportIngestJobInProgressWhenFailedAndExpectingRetry() {
            // Given
            Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
            Instant failTime = Instant.parse("2022-09-22T13:34:10.001Z");

            // When
            IngestJobStatus status = ingestJobStatus(job,
                    failedIngestRun(job, "test-task", new ProcessRunTime(startTime, failTime),
                            List.of("Failed reading input file", "Some IO failure")));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(true);
        }

        @Test
        public void shouldReportIngestJobFinishedWhenFailedAndRetried() {
            // Given
            Instant startTime1 = Instant.parse("2022-09-22T13:33:10.001Z");
            Instant startTime2 = Instant.parse("2022-09-22T13:34:15.001Z");
            RecordsProcessed recordsProcessed = new RecordsProcessed(123L, 100L);

            // When
            IngestJobStatus status = ingestJobStatus(job,
                    finishedIngestRun(job, "test-task", new RecordsProcessedSummary(
                            recordsProcessed, new ProcessRunTime(startTime2, Duration.ofMinutes(1)))),
                    failedIngestRun(job, "test-task",
                            new ProcessRunTime(startTime1, Duration.ofMinutes(1)),
                            List.of("Failed reading input file", "Some IO failure")));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(false);
        }

        @Test
        public void shouldReportIngestJobInProgressWhenFinishedButAnotherRunInProgress() {
            // Given
            Instant startTime1 = Instant.parse("2022-09-22T13:33:10.001Z");
            Instant startTime2 = Instant.parse("2022-09-22T13:33:15.001Z");
            RecordsProcessed recordsProcessed = new RecordsProcessed(123L, 100L);

            // When
            IngestJobStatus status = ingestJobStatus(job,
                    startedIngestRun(job, "task-2", startTime2),
                    finishedIngestRun(job, "task-1", new RecordsProcessedSummary(
                            recordsProcessed, new ProcessRunTime(startTime1, Duration.ofMinutes(1)))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(true);
        }

        @Test
        public void shouldReportIngestJobInProgressWhenFinishedButUncommitted() {
            // Given
            Instant startTime1 = Instant.parse("2022-09-22T13:33:10.001Z");
            Instant startTime2 = Instant.parse("2022-09-22T13:33:15.001Z");
            RecordsProcessed recordsProcessed = new RecordsProcessed(123L, 100L);

            // When
            IngestJobStatus status = ingestJobStatus(job,
                    startedIngestRun(job, "task-2", startTime2),
                    finishedIngestRunUncommitted(job, "task-1", new RecordsProcessedSummary(
                            recordsProcessed, new ProcessRunTime(startTime1, Duration.ofMinutes(1))), 1));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                    .isEqualTo(true);
        }
    }

    @Nested
    @DisplayName("Report furthest status")
    class ReportFurthestStatus {
        @Test
        void shouldReportValidatedWithOneRun() {
            // Given
            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("test-run",
                            acceptedStatusUpdate(Instant.parse("2022-09-22T13:33:10.001Z")))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(ACCEPTED);
        }

        @Test
        void shouldReportStartedWithOneRun() {
            // Given
            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run",
                            acceptedStatusUpdate(Instant.parse("2022-09-22T13:33:10Z"))),
                    forRunOnTask("run", "task",
                            startedStatusUpdateAfterValidation(Instant.parse("2022-09-22T13:33:11Z")))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldReportFinishedWithOneRun() {
            Instant validationTime = Instant.parse("2022-09-22T13:33:10Z");
            Instant startTime = Instant.parse("2022-09-22T13:33:11Z");
            Instant finishTime = Instant.parse("2022-09-22T13:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run", acceptedStatusUpdate(validationTime)),
                    forRunOnTask("run", "task",
                            startedStatusUpdateAfterValidation(startTime),
                            finishedStatusUpdate(startTime, finishTime))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(FINISHED);
        }

        @Test
        void shouldReportStartedWhenAnotherRunAcceptedAfterwards() {
            // Given
            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run-1",
                            acceptedStatusUpdate(Instant.parse("2022-09-22T13:33:00Z"))),
                    forRunOnTask("run-1", "task",
                            startedStatusUpdateAfterValidation(Instant.parse("2022-09-22T13:33:10Z"))),
                    forRunOnNoTask("run-2",
                            acceptedStatusUpdate(Instant.parse("2022-09-22T13:34:00Z")))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldReportStartedWhenAnotherRunAcceptedBefore() {
            // Given
            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run-1",
                            acceptedStatusUpdate(Instant.parse("2022-09-22T13:33:00Z"))),
                    forRunOnNoTask("run-2",
                            acceptedStatusUpdate(Instant.parse("2022-09-22T13:34:00Z"))),
                    forRunOnTask("run-2", "task",
                            startedStatusUpdateAfterValidation(Instant.parse("2022-09-22T13:34:10Z")))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldReportFailedWithOneRun() {
            Instant validationTime1 = Instant.parse("2022-09-22T13:33:10Z");
            Instant startTime = Instant.parse("2022-09-22T13:33:11Z");
            Instant finishTime = Instant.parse("2022-09-22T13:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run-1", acceptedStatusUpdate(validationTime1)),
                    forRunOnTask("run-1", "task",
                            startedStatusUpdateAfterValidation(startTime),
                            failedStatusUpdate(startTime, finishTime))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(FAILED);
        }

        @Test
        void shouldReportFinishedWhenAnotherRunFailedAfterwards() {
            Instant validationTime1 = Instant.parse("2022-09-22T13:33:10Z");
            Instant startTime1 = Instant.parse("2022-09-22T13:33:11Z");
            Instant finishTime1 = Instant.parse("2022-09-22T13:40:10Z");
            Instant validationTime2 = Instant.parse("2022-09-22T14:33:10Z");
            Instant startTime2 = Instant.parse("2022-09-22T14:33:11Z");
            Instant finishTime2 = Instant.parse("2022-09-22T14:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run-1", acceptedStatusUpdate(validationTime1)),
                    forRunOnTask("run-1", "task",
                            startedStatusUpdateAfterValidation(startTime1),
                            finishedStatusUpdate(startTime1, finishTime1)),
                    forRunOnNoTask("run-2", acceptedStatusUpdate(validationTime2)),
                    forRunOnTask("run-2", "task",
                            startedStatusUpdateAfterValidation(startTime2),
                            failedStatusUpdate(startTime2, finishTime2))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(FINISHED);
        }

        @Test
        void shouldReportInProgressWhenRetryingAfterFailure() {
            Instant validationTime1 = Instant.parse("2022-09-22T13:33:10Z");
            Instant startTime1 = Instant.parse("2022-09-22T13:33:11Z");
            Instant finishTime1 = Instant.parse("2022-09-22T13:40:10Z");
            Instant validationTime2 = Instant.parse("2022-09-22T14:33:10Z");
            Instant startTime2 = Instant.parse("2022-09-22T14:33:11Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run-1", acceptedStatusUpdate(validationTime1)),
                    forRunOnTask("run-1", "task",
                            startedStatusUpdateAfterValidation(startTime1),
                            failedStatusUpdate(startTime1, finishTime1)),
                    forRunOnNoTask("run-2", acceptedStatusUpdate(validationTime2)),
                    forRunOnTask("run-2", "task",
                            startedStatusUpdateAfterValidation(startTime2))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldReportInProgressWhenFileAddedButNotFinished() {
            Instant startTime = Instant.parse("2022-09-22T13:33:11Z");
            Instant writtenTime = Instant.parse("2022-09-22T13:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnTask("some-run", "some-task",
                            startedStatusUpdate(startTime),
                            filesAddedStatusUpdate(writtenTime, 1))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(IN_PROGRESS);
        }

        @Test
        void shouldReportUncommittedWhenFinishedAndNoFilesAdded() {
            Instant startTime = Instant.parse("2022-09-22T13:33:11Z");
            Instant finishTime = Instant.parse("2022-09-22T13:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnTask("some-run", "some-task",
                            startedStatusUpdate(startTime),
                            finishedStatusUpdateExpectingFileCommits(startTime, finishTime, 1))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(UNCOMMITTED);
        }

        @Test
        void shouldReportUncommittedWhenFinishedAndSomeFilesAdded() {
            Instant startTime = Instant.parse("2022-09-22T13:33:11Z");
            Instant writtenTime = Instant.parse("2022-09-22T13:39:10Z");
            Instant finishTime = Instant.parse("2022-09-22T13:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnTask("some-run", "some-task",
                            startedStatusUpdate(startTime),
                            filesAddedStatusUpdate(writtenTime, 1),
                            finishedStatusUpdateExpectingFileCommits(startTime, finishTime, 2))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(UNCOMMITTED);
        }

        @Test
        void shouldReportFinishedWhenAllFilesAdded() {
            Instant startTime = Instant.parse("2022-09-22T13:33:11Z");
            Instant writtenTime = Instant.parse("2022-09-22T13:39:10Z");
            Instant finishTime = Instant.parse("2022-09-22T13:40:10Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnTask("some-run", "some-task",
                            startedStatusUpdate(startTime),
                            filesAddedStatusUpdate(writtenTime, 2),
                            finishedStatusUpdateExpectingFileCommits(startTime, finishTime, 2))));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestRunStatusType)
                    .isEqualTo(FINISHED);
        }
    }

    @Nested
    @DisplayName("Apply expiry date")
    class ApplyExpiry {

        @Test
        public void shouldSetExpiryDateFromFirstRecord() {
            Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");
            Instant startExpiryTime = Instant.parse("2022-12-21T15:28:42.001Z");
            Instant finishTime = Instant.parse("2022-12-14T15:29:42.001Z");
            Instant finishExpiryTime = Instant.parse("2022-12-21T15:29:42.001Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forJob(job.getId(), withExpiry(startExpiryTime,
                            startedStatusUpdate(startTime))),
                    forJob(job.getId(), withExpiry(finishExpiryTime,
                            finishedStatusUpdate(startTime, finishTime)))));

            assertThat(status.getExpiryDate()).isEqualTo(startExpiryTime);
        }

        @Test
        public void shouldIgnoreJobWithoutStartedUpdateAsItMayHaveExpired() {
            Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");
            Instant finishTime = Instant.parse("2022-12-14T15:29:42.001Z");

            List<IngestJobStatus> statuses = jobStatusListFrom(records().fromUpdates(
                    forJob(job.getId(), finishedStatusUpdate(startTime, finishTime))));

            assertThat(statuses).isEmpty();
        }
    }

    private IngestJobAcceptedStatus acceptedStatusUpdate(Instant validationTime) {
        return IngestJobStatusTestHelper.ingestAcceptedStatus(validationTime);
    }

    private IngestJobStartedStatus startedStatusUpdate(Instant startTime) {
        return IngestJobStatusTestHelper.ingestStartedStatus(startTime, defaultUpdateTime(startTime));
    }

    private IngestJobStartedStatus startedStatusUpdateAfterValidation(Instant startTime) {
        return IngestJobStatusTestHelper.validatedIngestStartedStatus(startTime);
    }

    private IngestJobAddedFilesStatus filesAddedStatusUpdate(Instant writtenTime, int fileCount) {
        return IngestJobStatusTestHelper.ingestAddedFilesStatus(writtenTime, fileCount);
    }

    private IngestJobFinishedStatus finishedStatusUpdate(Instant startTime, Instant finishTime) {
        return IngestJobStatusTestHelper.ingestFinishedStatus(summary(startTime, finishTime), 2);
    }

    private IngestJobFinishedStatus finishedStatusUpdateExpectingFileCommits(
            Instant startTime, Instant finishTime, int numFilesAddedByJob) {
        return IngestJobStatusTestHelper.ingestFinishedStatusUncommitted(summary(startTime, finishTime), numFilesAddedByJob);
    }

    private ProcessFailedStatus failedStatusUpdate(Instant startTime, Instant finishTime) {
        return ProcessFailedStatus.timeAndReasons(
                defaultUpdateTime(finishTime), new ProcessRunTime(startTime, finishTime),
                List.of("Something went wrong"));
    }

    private RecordsProcessedSummary summary(Instant startTime, Instant finishTime) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);
    }
}
