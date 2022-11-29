/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class JobRunsTest {
    private static final String DEFAULT_TASK_ID_1 = "task-id-1";
    private static final String DEFAULT_TASK_ID_2 = "task-id-2";

    @Test
    public void shouldReportNoFinishedStatusWhenJobNotFinished() {
        // Given
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportRunWhenJobFinished() {
        // Given
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started, finished)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started, finished));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportTwoRunsLatestFirstByStartTimeOnSameTask() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:24:30.012Z"),
                Instant.parse("2022-09-24T09:24:30.001Z"));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, started2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started2, null),
                        tuple(DEFAULT_TASK_ID_1, started1, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportTwoRunsWhenJobFinishedMultipleTimesSameTask() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1,
                        started1, finished1, started2, finished2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportTwoTasksWithTwoRunsEachForSameJobWithInterleavingStartTimes() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started3 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started4 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, started3)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, started4)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started4, null),
                        tuple(DEFAULT_TASK_ID_1, started3, null),
                        tuple(DEFAULT_TASK_ID_2, started2, null),
                        tuple(DEFAULT_TASK_ID_1, started1, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportTwoTasksWithOneFinishedRunEachForSameJob() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, finished1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, finished2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportRunWhenJobFinishedReturnedFromDatabaseOutOfOrder() {
        // Given
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, finished, started)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(started, finished));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportRunsWhenJobStartedReturnedFromDatabaseOutOfOrder() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started3 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started3, started1, started2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, null),
                        tuple(started2, null),
                        tuple(started1, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportRunsWhenLastRunFinishedButReturnedFromDatabaseOutOfOrder() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started3 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));
        ProcessFinishedStatus finished = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-27T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-26T09:23:30.001Z"),
                        Instant.parse("2022-09-27T09:24:00.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started3, finished, started1, started2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, finished),
                        tuple(started2, null),
                        tuple(started1, null));
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldReportRunsOnDifferentTasksWhenJobFinishedFromDatabaseOutOfOrder() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-22T09:23:30.012Z"),
                Instant.parse("2022-09-22T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-22T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-22T09:23:30.001Z"),
                        Instant.parse("2022-09-22T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-22T09:23:31.012Z"),
                Instant.parse("2022-09-22T09:23:31.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-22T09:24:01.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-22T09:23:31.001Z"),
                        Instant.parse("2022-09-22T09:24:01.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, finished1, started1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, finished2, started2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldReportRunsOnDifferentTasksWhenJobRunStartedAndFinshedDuringAnotherRun() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-25T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-25T09:23:30.001Z"),
                        Instant.parse("2022-09-25T09:24:00.001Z")));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-26T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-26T09:23:30.001Z"),
                        Instant.parse("2022-09-26T09:24:00.001Z")));

        // When
        ProcessRuns runs = new TestProcessStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, finished1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, finished2)
                .buildRuns();

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(runs.isFinished()).isTrue();
    }
}
