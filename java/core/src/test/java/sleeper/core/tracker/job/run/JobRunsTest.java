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

import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobRunFinishedStatus;
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
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.TASK_ID_1;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.TASK_ID_2;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJobRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forRunOnNoTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.onNoTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.onTask;

class JobRunsTest {

    @DisplayName("Report start and finish of a job")
    @Nested
    class ReportStartAndFinish {

        @Test
        void shouldReportNoFinishedStatusWhenJobNotFinished() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(started);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started, null));
        }

        @Test
        void shouldReportRunWhenJobFinished() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            JobRunFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(started, finished);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started, finished));
        }

        @Test
        void shouldIncludeExtraFinishedStatus() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            JobRunFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
            JobRunFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

            // When
            JobRuns runs = runsFromUpdates(started, finished1, finished2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started, finished2, List.of(started, finished1, finished2)));
        }

        @Test
        void shouldReportRunFailedBeforeStart() {
            // Given
            JobRunFailedStatus failed = failedStatus(Instant.parse("2022-09-24T09:23:30.001Z"), Duration.ofSeconds(30), List.of("Job could not be started"));

            // When
            JobRuns runs = runsFromUpdates(forJobRunOnTask("some-job", "some-run", "some-task", failed));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getStartedStatus, JobRun::getStartTime, JobRun::getStartUpdateTime, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(null, null, null, failed));
        }
    }

    @DisplayName("Correlate job runs by task ID and time")
    @Nested
    class CorrelateRunsByTaskAndTime {

        @Test
        void shouldReportTwoRunsLatestFirstByStartTimeOnSameTask() {
            // Given
            TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:24:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(started1, started2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started2, null),
                            tuple(DEFAULT_TASK_ID, started1, null));
        }

        @Test
        void shouldReportTwoRunsWhenJobFinishedMultipleTimesSameTask() {
            // Given
            TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            JobRunFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
            TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            JobRunFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(started1, finished1, started2, finished2);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(DEFAULT_TASK_ID, started2, finished2),
                            tuple(DEFAULT_TASK_ID, started1, finished1));
        }

        @Test
        void shouldReportTwoTasksWithTwoRunsEachForSameJobWithInterleavingStartTimes() {
            // Given
            TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            TestJobStartedStatus started3 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));
            TestJobStartedStatus started4 = startedStatus(Instant.parse("2022-09-26T09:23:30.001Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started1, started3),
                    onTask(TASK_ID_2, started2, started4));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(TASK_ID_2, started4, null),
                            tuple(TASK_ID_1, started3, null),
                            tuple(TASK_ID_2, started2, null),
                            tuple(TASK_ID_1, started1, null));
        }

        @Test
        void shouldReportTwoTasksWithOneFinishedRunEach() {
            // Given
            TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            JobRunFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
            TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
            JobRunFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started1, finished1),
                    onTask(TASK_ID_2, started2, finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(TASK_ID_2, started2, finished2),
                            tuple(TASK_ID_1, started1, finished1));
        }

        @Test
        void shouldReportRunsOnDifferentTasksWhenJobRunStartedAndFinishedDuringAnotherRun() {
            // Given
            TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:00.001Z"));
            TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
            JobRunFinishedStatus finished1 = finishedStatus(started1, Duration.ofMinutes(2), 450L, 300L);
            JobRunFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started1, finished1),
                    onTask(TASK_ID_2, started2, finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                    .containsExactly(
                            tuple(TASK_ID_2, started2, finished2),
                            tuple(TASK_ID_1, started1, finished1));
        }

        @Test
        void shouldExcludeUpdateNotPartOfARunBeforeTask() {
            // Given
            TestJobStatus notPartOfRun = notPartOfRunWithUpdateTime(Instant.parse("2024-06-19T14:06:00Z"));
            TestJobStartedStatus started = startedStatus(Instant.parse("2024-06-19T14:06:01Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    onNoTask(notPartOfRun),
                    onTask(TASK_ID_1, started));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(TASK_ID_1, List.of(started)));
        }

        @Test
        void shouldExcludeUpdateNotPartOfARunAfterTaskStarted() {
            // Given
            TestJobStartedStatus started = startedStatus(Instant.parse("2024-06-19T14:06:00Z"));
            TestJobStatus notPartOfRun = notPartOfRunWithUpdateTime(Instant.parse("2024-06-19T14:06:01Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    onTask(TASK_ID_1, started),
                    onNoTask(notPartOfRun));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple(TASK_ID_1, List.of(started)));
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
            JobRunFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
            JobRunFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

            // When
            JobRuns runs = runsFromUpdates(
                    forRunOnTask("run-1", "some-task", started),
                    forRunOnTask("run-1", "some-task", finished1),
                    forRunOnTask("run-1", "some-task", finished2));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", started, finished2, List.of(started, finished1, finished2)));
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
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", started, null, List.of(update, started)));
        }

        @Test
        void shouldIncludeUpdateForRunAfterItIsFinished() {
            // Given
            TestJobStartedStatus started = startedStatus(
                    Instant.parse("2024-06-19T13:26:00Z"));
            JobRunFinishedStatus finished = finishedStatus(
                    started, Duration.ofMinutes(1), 123, 123);
            TestJobStatus update = partOfRunWithUpdateTime(
                    Instant.parse("2024-06-19T13:28:00Z"));

            // When
            JobRuns runs = runsFromUpdates(
                    forRunOnTask("some-run", "some-task", started, finished),
                    forRunOnNoTask("some-run", update));

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus, JobRun::getStatusUpdates)
                    .containsExactly(
                            tuple("some-task", started, finished, List.of(started, finished, update)));
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

    @DisplayName("Flag updates as part/start of a run")
    @Nested
    class FlagUpdatesAsPartOrStartOfRun {

        @Test
        void shouldNotCreateRunIfStatusUpdateNotFlaggedAsStartOfRun() {
            // Given
            JobStatusUpdate notStartedUpdate = () -> Instant.parse("2022-09-24T09:23:30.001Z");

            // When
            JobRuns runs = runsFromUpdates(notStartedUpdate);

            // Then
            assertThat(runs.getRunsLatestFirst()).isEmpty();
        }

        @Test
        void shouldCreateRunFromTwoStartedUpdatesWhenStartOfRunIsAfterTheOther() {
            // Given
            TestJobStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = startedStatusNotStartOfRun(
                    Instant.parse("2022-09-24T08:23:30Z"));
            TestJobStartedStatus startedStatus = startedStatus(
                    Instant.parse("2022-09-24T09:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(startedStatusNotStartOfRun, startedStatus);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .containsExactly(JobRun.started(DEFAULT_TASK_ID, startedStatus));
        }

        @Test
        void shouldCreateRunFromTwoStartedUpdatesWhenStartOfRunIsBeforeTheOther() {
            // Given
            TestJobStartedStatus startedStatus = startedStatus(
                    Instant.parse("2022-09-24T09:23:30Z"));
            TestJobStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = startedStatusNotStartOfRun(
                    Instant.parse("2022-09-24T10:23:30Z"));

            // When
            JobRuns runs = runsFromUpdates(startedStatus, startedStatusNotStartOfRun);

            // Then
            assertThat(runs.getRunsLatestFirst())
                    .containsExactly(JobRun.builder()
                            .taskId(DEFAULT_TASK_ID)
                            .startedStatus(startedStatus)
                            .statusUpdate(startedStatusNotStartOfRun)
                            .build());
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
                    .containsExactly(JobRun.builder()
                            .taskId(DEFAULT_TASK_ID)
                            .startedStatus(startedStatus)
                            .statusUpdate(customStatus)
                            .build());
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
                    .containsExactly(JobRun.started("a-task", startedStatus));
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
                    .containsExactly(JobRun.started("a-task", startedStatus));
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
                    .containsExactly(JobRun.started(DEFAULT_TASK_ID, status));
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
            JobRuns runs = runsFromUpdates(startedAndFinished, started);

            // Then
            assertThat(runs.getRunsLatestFirst()).containsExactly(
                    JobRun.started(DEFAULT_TASK_ID, started),
                    JobRun.started(DEFAULT_TASK_ID, startedAndFinished));
            assertThat(runs.isStarted()).isTrue();
        }

        @Test
        void shouldFinishAnotherRunAfterAStartedAndFinishedUpdate() {
            // Given
            TestJobStartedAndFinishedStatus startedAndFinished = TestJobStartedAndFinishedStatus.updateAndSummary(
                    Instant.parse("2022-09-24T09:23:30.123Z"),
                    summary(Instant.parse("2022-09-24T09:23:30Z"), Duration.ZERO, 0L, 0L));
            TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:24:00.001Z"));
            JobRunFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

            // When
            JobRuns runs = runsFromUpdates(startedAndFinished, started, finished);

            // Then
            assertThat(runs.getRunsLatestFirst()).containsExactly(
                    JobRun.finished(DEFAULT_TASK_ID, started, finished),
                    JobRun.started(DEFAULT_TASK_ID, startedAndFinished));
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
