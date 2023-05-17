/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.core.record.process.status;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.core.record.process.status.CustomProcessStatus.notPartOfRunWithUpdateTime;
import static sleeper.core.record.process.status.CustomProcessStatus.partOfRunWithUpdateTime;
import static sleeper.core.record.process.status.ProcessStartedStatus.updateAndStartTime;
import static sleeper.core.record.process.status.ProcessStartedStatusWithStartOfRunFlag.updateAndStartTimeNotStartOfRun;
import static sleeper.core.record.process.status.TestProcessRuns.runsFromUpdates;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.DEFAULT_TASK_ID;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.TASK_ID_1;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.TASK_ID_2;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.onTask;
import static sleeper.core.record.process.status.TestRunStatusUpdates.finishedStatus;
import static sleeper.core.record.process.status.TestRunStatusUpdates.startedStatus;

public class ProcessRunsTest {

    @Test
    public void shouldReportNoFinishedStatusWhenJobNotFinished() {
        // Given
        ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        ProcessRuns runs = runsFromUpdates(started);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportRunWhenJobFinished() {
        // Given
        ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

        // When
        ProcessRuns runs = runsFromUpdates(started, finished);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, finished));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportTwoRunsLatestFirstByStartTimeOnSameTask() {
        // Given
        ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:24:30.001Z"));

        // When
        ProcessRuns runs = runsFromUpdates(started1, started2);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started2, null),
                        tuple(DEFAULT_TASK_ID, started1, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportTwoRunsWhenJobFinishedMultipleTimesSameTask() {
        // Given
        ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
        ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

        // When
        ProcessRuns runs = runsFromUpdates(started1, finished1, started2, finished2);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started2, finished2),
                        tuple(DEFAULT_TASK_ID, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportTwoTasksWithTwoRunsEachForSameJobWithInterleavingStartTimes() {
        // Given
        ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started3 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started4 = startedStatus(Instant.parse("2022-09-26T09:23:30.001Z"));

        // When
        ProcessRuns runs = runsFromUpdates(
                onTask(TASK_ID_1, started1, started3),
                onTask(TASK_ID_2, started2, started4));

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(TASK_ID_2, started4, null),
                        tuple(TASK_ID_1, started3, null),
                        tuple(TASK_ID_2, started2, null),
                        tuple(TASK_ID_1, started1, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportTwoTasksWithOneFinishedRunEach() {
        // Given
        ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
        ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

        // When
        ProcessRuns runs = runsFromUpdates(
                onTask(TASK_ID_1, started1, finished1),
                onTask(TASK_ID_2, started2, finished2));

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(TASK_ID_2, started2, finished2),
                        tuple(TASK_ID_1, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportRunsOnDifferentTasksWhenJobRunStartedAndFinishedDuringAnotherRun() {
        // Given
        ProcessStartedStatus started1 = startedStatus(Instant.parse("2022-09-23T09:23:00.001Z"));
        ProcessStartedStatus started2 = startedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = finishedStatus(started1, Duration.ofMinutes(2), 450L, 300L);
        ProcessFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

        // When
        ProcessRuns runs = runsFromUpdates(
                onTask(TASK_ID_1, started1, finished1),
                onTask(TASK_ID_2, started2, finished2));

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(TASK_ID_2, started2, finished2),
                        tuple(TASK_ID_1, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldIgnoreExtraFinishedStatus() {
        // Given
        ProcessStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = finishedStatus(started, Duration.ofSeconds(30), 100, 100);
        ProcessFinishedStatus finished2 = finishedStatus(started, Duration.ofSeconds(40), 200, 200);

        // When
        ProcessRuns runs = runsFromUpdates(started, finished1, finished2);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    void shouldNotCreateProcessRunIfStatusUpdateNotFlaggedAsStartOfRun() {
        // Given
        ProcessStatusUpdate notStartedUpdate = () -> Instant.parse("2022-09-24T09:23:30.001Z");

        // When
        ProcessRuns runs = runsFromUpdates(notStartedUpdate);

        // Then
        assertThat(runs.getRunList()).isEmpty();
    }

    @Test
    void shouldCreateProcessRunFromTwoStartedUpdatesWhenOneIsFlaggedAsStartOfRun() {
        // Given
        ProcessStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun = updateAndStartTimeNotStartOfRun(
                Instant.parse("2022-09-24T08:23:30Z"), Instant.parse("2022-09-24T08:23:30.001Z"));
        ProcessStartedStatus startedStatus = updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30Z"), Instant.parse("2022-09-24T09:23:30.001Z"));

        // When
        ProcessRuns runs = runsFromUpdates(startedStatusNotStartOfRun, startedStatus);

        // Then
        assertThat(runs.getRunList())
                .containsExactly(ProcessRun.started(DEFAULT_TASK_ID, startedStatus));
    }

    @Test
    void shouldCreateProcessRunWithCustomStatusUpdatePartOfRun() {
        // Given
        ProcessStartedStatus startedStatus = updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30Z"), Instant.parse("2022-09-24T09:23:30.001Z"));
        CustomProcessStatus customStatus = partOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

        // When
        ProcessRuns runs = runsFromUpdates(startedStatus, customStatus);

        // Then
        assertThat(runs.getRunList())
                .containsExactly(ProcessRun.builder()
                        .taskId(DEFAULT_TASK_ID)
                        .startedStatus(startedStatus)
                        .statusUpdate(customStatus)
                        .build());
    }

    @Test
    void shouldCreateProcessRunWithCustomStatusUpdateNotPartOfRun() {
        // Given
        ProcessStartedStatus startedStatus = updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30Z"), Instant.parse("2022-09-24T09:23:30.001Z"));
        CustomProcessStatus customStatus = notPartOfRunWithUpdateTime(Instant.parse("2022-09-24T10:23:30Z"));

        // When
        ProcessRuns runs = runsFromUpdates(startedStatus, customStatus);

        // Then
        assertThat(runs.getRunList())
                .containsExactly(ProcessRun.started(DEFAULT_TASK_ID, startedStatus));
    }
}
