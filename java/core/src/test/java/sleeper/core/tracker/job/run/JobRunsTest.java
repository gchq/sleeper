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

package sleeper.core.tracker.job.run;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.tracker.job.status.AggregatedTaskJobsFinishedStatus;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;
import sleeper.core.tracker.job.status.TestJobStartedAndFinishedStatus;
import sleeper.core.tracker.job.status.TestJobStartedStatus;
import sleeper.core.tracker.job.status.TestJobStartedStatusWithStartOfRunFlag;
import sleeper.core.tracker.job.status.TestJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunsTestHelper.runsFromUpdates;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.failedStatus;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.finishedStatus;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.startedStatus;
import static sleeper.core.tracker.job.status.TestJobStartedStatusWithStartOfRunFlag.startedStatusNotStartOfRun;
import static sleeper.core.tracker.job.status.TestJobStatus.notPartOfRunWithUpdateTime;
import static sleeper.core.tracker.job.status.TestJobStatus.partOfRunWithUpdateTime;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.DEFAULT_TASK_ID;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJobRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forNoRunNoTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forRunOnNoTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.onNoTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.onTask;

class JobRunsTest {

    @DisplayName("Gather updates for one run")
    @Nested
    class OneRun {

        @Test
        void shouldReportNoFinishedStatusWhenJobNotFinished() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(started);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, List.of(started)));
        }

        @Test
        void shouldReportRunWhenJobFinished() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            AggregatedTaskJobsFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(started, finished);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, List.of(started, finished)));
        }

        @Test
        void shouldIncludeExtraFinishedStatus() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
            AggregatedTaskJobsFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

            // When
            JobRuns runs = runsFromUpdates(started, finished1, finished2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, List.of(started, finished1, finished2)));
        }

        @Test
        void shouldReportRunFailedBeforeStart() {
            // Given
            JobRunFailedStatus failed = failedStatus(Instant.parse("2022-09-24T09:24:00.001Z"), List.of("Job could not be started"));

            // When
            JobRuns runs = runsFromUpdates(forJobRunOnTask("some-job", "some-run", "some-task", failed));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(failed)));
        }
    }

    @DisplayName("Correlate job runs by run ID")
    @Nested
    class CorrelateRunsById {

        @Test
        void shouldReportTwoRunsLatestFirstWhenAnEventHappensForBothBeforeEitherAreOnATask() {
            // Given
            TestJobStartedStatus validated1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            TestJobStartedStatus validated2 = startedStatus(Instant.parse("2022-09-24T09:24:30.001Z"));

            TestJobStartedStatusWithStartOfRunFlag started1 = startedStatusNotStartOfRun(Instant.parse("2022-09-24T10:23:30Z"));
            TestJobStartedStatusWithStartOfRunFlag started2 = startedStatusNotStartOfRun(Instant.parse("2022-09-24T10:24:30Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    forRunOnNoTask("run-1", validated1),
                    forRunOnNoTask("run-2", validated2),
                    forRunOnTask("run-1", "some-task", started1),
                    forRunOnTask("run-2", "some-task", started2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(validated2, started2)),
                            tuple("some-task", List.of(validated1, started1)));
        }

        @Test
        void shouldIncludeExtraFinishedStatus() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            AggregatedTaskJobsFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
            AggregatedTaskJobsFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

            // When
            JobRuns runs = runsFromUpdates(
                    forRunOnTask("run-1", "some-task", started),
                    forRunOnTask("run-1", "some-task", finished1),
                    forRunOnTask("run-1", "some-task", finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(started, finished1, finished2)));
        }

        @Test
        void shouldIncludeUpdateForRunBeforeStartTimeWhenOccurredOnAnotherProcessWithOutOfSyncClock() {
            // Given a started status, and a status update that occurred on another process
            // And the other process has an out of sync clock such that the update occurred before the start update
            TestJobStartedStatus started = startedStatus(
                    Instant.parse("2024-06-19T13:26:00Z"));
            TestJobStatus update = partOfRunWithUpdateTime(
                    Instant.parse("2024-06-19T13:25:59Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    forRunOnTask("some-run", "some-task", started),
                    forRunOnNoTask("some-run", update));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(update, started)));
        }

        @Test
        void shouldIncludeUpdateForRunAfterItIsFinished() {
            // Given
            TestJobStartedStatus started = startedStatus(
                    Instant.parse("2024-06-19T13:26:00Z"));
            AggregatedTaskJobsFinishedStatus finished = finishedStatus(
                    started, Duration.ofMinutes(1), 123, 123);
            TestJobStatus update = partOfRunWithUpdateTime(
                    Instant.parse("2024-06-19T13:28:00Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    forRunOnTask("some-run", "some-task", started, finished),
                    forRunOnNoTask("some-run", update));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(started, finished, update)));
        }

        @Test
        void shouldExcludeUpdateNotPartOfARun() {
            // Given
            TestJobStatus notPartOfRun = notPartOfRunWithUpdateTime(Instant.parse("2024-06-19T14:06:00Z"));
            TestJobStartedStatus started = startedStatus(Instant.parse("2024-06-19T14:06:01Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    onNoTask(notPartOfRun),
                    forRunOnTask("some-run", "some-task", started));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", List.of(started)));
        }
    }

    @DisplayName("Report task assignment")
    @Nested
    class ReportTaskAssignment {

        @Test
        void shouldReportAssignedToTask() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(onTask("some-task", started));

            // Then
            assertThat(runs.isTaskIdAssigned("some-task"))
                    .isTrue();
        }

        @Test
        void shouldReportNotAssignedToTaskWhenOnAnotherTask() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(onTask("other-task", started));

            // Then
            assertThat(runs.isTaskIdAssigned("some-task"))
                    .isFalse();
        }

        @Test
        void shouldReportNotAssignedToTaskWhenOnNoTask() {
            // Given
            TestJobStartedStatus validated = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(onNoTask(validated));

            // Then
            assertThat(runs.isTaskIdAssigned("some-task"))
                    .isFalse();
        }
    }

    @DisplayName("Flag updates as part of a run")
    @Nested
    class FlagUpdatesAsPartOfRun {

        @Test
        void shouldNotCreateRunIfStatusUpdateNotFlaggedAsPartOfRun() {
            // Given
            JobStatusUpdate notStartedUpdate = notPartOfRunWithUpdateTime(Instant.parse("2022-09-24T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(notStartedUpdate);

            // Then
            assertThat(runs.getRunsLatestFirst()).isEmpty();
        }

        @Test
        void shouldCreateRunWithCustomStatusUpdatePartOfRun() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStatus customStatus = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(startedStatus, customStatus);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .singleElement()
                    .extracting(JobRun::getStatusUpdates)
                    .isEqualTo(List.of(startedStatus, customStatus));
        }

        @Test
        void shouldCreateRunWithCustomStatusUpdateNotPartOfRun() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStatus customStatus = notPartOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(onTask("a-task", startedStatus), onNoTask(customStatus));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .singleElement()
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly("a-task", List.of(startedStatus));
        }

        @Test
        void shouldCreateRunWithCustomStatusUpdateNotPartOfRunButStillOnTask() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStatus customStatus = notPartOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(onTask("a-task", startedStatus, customStatus));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .singleElement()
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly("a-task", List.of(startedStatus));
        }

        @Test
        void shouldCreateRunWhenStatusUpdatePartOfRunButHasNoRunId() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(forNoRunNoTask(startedStatus));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .singleElement()
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(null, List.of(startedStatus));
        }

        @Test
        void shouldCreateTwoRunsWhenStatusUpdatePartOfRunButHasNoRunId() {
            // Given
            TestJobStartedStatus startedStatus1 = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStartedStatus startedStatus2 = startedStatus(Instant.parse("2022-09-24T09:24:30Z"));

            // When
            JobRuns runs = runsFromUpdates(forNoRunNoTask(startedStatus1), forNoRunNoTask(startedStatus2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(null, List.of(startedStatus2)),
                            tuple(null, List.of(startedStatus1)));
        }
    }

    @DisplayName("An update can be the start and end of a run")
    @Nested
    class UpdateCanBeStartAndEndOfRun {

        @Test
        void shouldCreateRunWithOneUpdateWhichIsStartAndFinish() {
            // Given
            TestJobStartedAndFinishedStatus status = TestJobStartedAndFinishedStatus.updateAndSummary(
                    Instant.parse("2022-09-24T09:23:30.123Z"),
                    summary(Instant.parse("2022-09-24T09:23:30Z"), Duration.ZERO, 0L, 0L));

            // When
            JobRuns runs = runsFromUpdates(status);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .singleElement()
                    .extracting(JobRun::getStatusUpdates)
                    .isEqualTo(List.of(status));
            assertThat(runs.isStarted()).isTrue();
        }

        @Test
        void shouldStartAnotherRunAfterAStartedAndFinishedUpdate() {
            // Given
            TestJobStartedAndFinishedStatus startedAndFinished = TestJobStartedAndFinishedStatus.updateAndSummary(
                    Instant.parse("2022-09-24T09:23:30.123Z"),
                    summary(Instant.parse("2022-09-24T09:23:30Z"), Duration.ZERO, 0L, 0L));
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:24:00.001Z"));

            // When
            JobRuns runs = runsFromUpdates(forRunOnTask(startedAndFinished), forRunOnTask(started));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getStatusUpdates)
                    .containsExactly(
                            List.of(started),
                            List.of(startedAndFinished));
            assertThat(runs.isStarted()).isTrue();
        }

        @Test
        void shouldFinishAnotherRunAfterAStartedAndFinishedUpdate() {
            // Given
            TestJobStartedAndFinishedStatus startedAndFinished = TestJobStartedAndFinishedStatus.updateAndSummary(
                    Instant.parse("2022-09-24T09:23:30.123Z"),
                    summary(Instant.parse("2022-09-24T09:23:30Z"), Duration.ZERO, 0L, 0L));
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:24:00.001Z"));
            AggregatedTaskJobsFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(forRunOnTask(startedAndFinished), forRunOnTask(started, finished));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getStatusUpdates)
                    .containsExactly(
                            List.of(started, finished),
                            List.of(startedAndFinished));
            assertThat(runs.isStarted()).isTrue();
        }
    }

    @Nested
    @DisplayName("Retrieve status updates by class")
    class RetrieveStatusUpdatesByClass {
        @Test
        void shouldReturnLastStatusUpdateByClass() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStatus customStatus = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(startedStatus, customStatus);

            // Then
            assertThat(runs.getLatestRun()
                    .flatMap(latestRun -> latestRun.getLastStatusOfType(TestJobStatus.class)))
                    .get().isEqualTo(customStatus);
        }

        @Test
        void shouldReturnLastStatusUpdateByClassWithMultipleUpdatesForClass() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStatus customStatus1 = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));
            TestJobStatus customStatus2 = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:25:30Z"));

            // When
            JobRuns runs = runsFromUpdates(startedStatus, customStatus1, customStatus2);

            // Then
            assertThat(runs.getLatestRun()
                    .flatMap(latestRun -> latestRun.getLastStatusOfType(TestJobStatus.class)))
                    .get().isEqualTo(customStatus2);
        }

        @Test
        void shouldReturnLastStatusUpdateByInterface() {
            TestJobStartedStatus startedUpdate = startedStatus(
                    Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = startedStatusNotStartOfRun(
                    Instant.parse("2022-09-24T10:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(startedUpdate, startedStatusNotStartOfRun);

            // Then
            assertThat(runs.getLatestRun()
                    .flatMap(latestRun -> latestRun.getLastStatusOfType(JobRunStartedUpdate.class)))
                    .get().isEqualTo(startedStatusNotStartOfRun);
        }
    }
}
