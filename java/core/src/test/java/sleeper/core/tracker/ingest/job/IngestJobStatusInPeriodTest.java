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

package sleeper.core.tracker.ingest.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.tracker.job.status.JobStatusUpdate;
import sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.TaskUpdates;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatusUncommitted;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.singleIngestJobStatusFrom;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

public class IngestJobStatusInPeriodTest {

    @Nested
    @DisplayName("Unfinished job")
    class UnfinishedJob {

        @Test
        public void shouldIncludeUnfinishedJobWhenStartedBeforeWindowStartTime() {
            // Given
            Instant jobStartTime = Instant.parse("2022-09-23T11:43:00.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(unfinishedStatus(jobStartTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }

        @Test
        public void shouldIncludeUnfinishedJobWhenStartedDuringWindow() {
            // Given
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant jobStartTime = Instant.parse("2022-09-23T11:44:30.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(unfinishedStatus(jobStartTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }

        @Test
        public void shouldNotIncludeUnfinishedJobWhenStartedAfterWindow() {
            // Given
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");
            Instant jobStartTime = Instant.parse("2022-09-23T11:45:30.000Z");

            // When / Then
            assertThat(unfinishedStatus(jobStartTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isFalse();
        }

        @Test
        public void shouldIncludeFinishedButUncommittedJobWhenFinishedBeforeWindow() {
            // Given
            Instant jobStartTime = Instant.parse("2022-09-23T11:44:30.000Z");
            Instant jobFinishedTime = Instant.parse("2022-09-23T11:45:30.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:46:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:47:00.000Z");

            // When / Then
            assertThat(finishedStatusUncommitted(jobStartTime, jobFinishedTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("Finished job")
    class FinishedJob {

        @Test
        public void shouldIncludeFinishedJobWhenRanInsideWindow() {
            // Given
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant jobStartTime = Instant.parse("2022-09-23T11:44:15.000Z");
            Instant jobFinishTime = Instant.parse("2022-09-23T11:44:45.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(finishedStatus(jobStartTime, jobFinishTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }

        @Test
        public void shouldNotIncludeFinishedJobWhenFinishedBeforeWindow() {
            // Given
            Instant jobStartTime = Instant.parse("2022-09-23T11:42:00.000Z");
            Instant jobFinishTime = Instant.parse("2022-09-23T11:43:00.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(finishedStatus(jobStartTime, jobFinishTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isFalse();
        }

        @Test
        public void shouldIncludeFinishedJobWhenOverlapsWithWindow() {
            // Given
            Instant jobStartTime = Instant.parse("2022-09-23T11:43:30.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant jobFinishTime = Instant.parse("2022-09-23T11:44:30.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(finishedStatus(jobStartTime, jobFinishTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("Job with multiple runs")
    class MultipleRunsJob {

        @Test
        public void shouldIncludeUnfinishedJobWhenOneRunFinishedBeforeWindow() {
            // Given
            Instant run1StartTime = Instant.parse("2022-09-23T11:43:15.000Z");
            Instant run1FinishTime = Instant.parse("2022-09-23T11:43:45.000Z");
            Instant run2StartTime = Instant.parse("2022-09-23T11:43:50.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(statusFromUpdates(
                    forRunOnTask(startedRun(run1StartTime), finishedRun(run1FinishTime)),
                    forRunOnTask(startedRun(run2StartTime)))
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }

        @Test
        public void shouldNotIncludeFinishedJobWhenTwoRunsFinishedBeforeWindow() {
            // Given
            Instant run1StartTime = Instant.parse("2022-09-23T11:43:15.000Z");
            Instant run1FinishTime = Instant.parse("2022-09-23T11:43:30.000Z");
            Instant run2StartTime = Instant.parse("2022-09-23T11:43:40.000Z");
            Instant run2FinishTime = Instant.parse("2022-09-23T11:43:55.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(statusFromUpdates(
                    forRunOnTask(startedRun(run1StartTime), finishedRun(run1FinishTime)),
                    forRunOnTask(startedRun(run2StartTime), finishedRun(run2FinishTime)))
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isFalse();
        }

        @Test
        public void shouldIncludeFinishedJobWhenSecondRunFinishedDuringWindow() {
            // Given
            Instant run1StartTime = Instant.parse("2022-09-23T11:43:15.000Z");
            Instant run1FinishTime = Instant.parse("2022-09-23T11:43:45.000Z");
            Instant run2StartTime = Instant.parse("2022-09-23T11:43:45.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant run2FinishTime = Instant.parse("2022-09-23T11:44:15.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(statusFromUpdates(
                    forRunOnTask(startedRun(run1StartTime), finishedRun(run1FinishTime)),
                    forRunOnTask(startedRun(run2StartTime), finishedRun(run2FinishTime)))
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }
    }

    private IngestJobStatus unfinishedStatus(Instant startTime) {
        return singleRunStatus(ingestStartedStatus(startTime));
    }

    private IngestJobStatus finishedStatus(Instant startTime, Instant finishTime) {
        return singleRunStatus(
                ingestStartedStatus(startTime),
                ingestFinishedStatus(summary(startTime, finishTime, 100, 100), 2));
    }

    private IngestJobStatus finishedStatusUncommitted(Instant startTime, Instant finishTime) {
        return singleRunStatus(
                ingestStartedStatus(startTime),
                ingestFinishedStatusUncommitted(summary(startTime, finishTime, 100, 100), 2));
    }

    private IngestJobStatus singleRunStatus(JobStatusUpdate... updates) {
        return singleIngestJobStatusFrom(records().singleRunUpdates(updates));
    }

    private IngestJobStatus statusFromUpdates(TaskUpdates... updates) {
        return singleIngestJobStatusFrom(records().fromUpdates(updates));
    }

    private JobStatusUpdate startedRun(Instant startedTime) {
        return IngestJobStatusTestData.ingestStartedStatus(startedTime, defaultUpdateTime(startedTime));
    }

    private JobStatusUpdate finishedRun(Instant finishedTime) {
        return IngestJobFinishedStatus.builder()
                .updateTime(defaultUpdateTime(finishedTime))
                .finishTime(finishedTime)
                .recordsProcessed(new RowsProcessed(100, 100))
                .numFilesWrittenByJob(2)
                .build();
    }
}
