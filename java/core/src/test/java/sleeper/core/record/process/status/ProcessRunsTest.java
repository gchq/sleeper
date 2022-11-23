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
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class ProcessRunsTest {

    @Test
    public void shouldBuildProcessRunsWhenOneProcessStarted() {
        // Given
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));

        // When
        ProcessRun run = ProcessRun.started("test-task-1", started);
        ProcessRuns runs = ProcessRuns.latestFirst(run);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple("test-task-1", started, null));
        assertThat(runs.isStarted()).isTrue();
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildProcessRunsWhenNoProcessesStarted() {
        // Given
        ProcessRuns runs = ProcessRuns.latestFirst(Collections.emptyList());

        // Then
        assertThat(runs.getRunList()).isEmpty();
        assertThat(runs.isStarted()).isFalse();
        assertThat(runs.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildProcessRunsWhenAllRunsFinished() {
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
        ProcessRun run = ProcessRun.finished("test-task-1", started, finished);
        ProcessRuns runs = ProcessRuns.latestFirst(run);

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple("test-task-1", started, finished));
        assertThat(runs.isStarted()).isTrue();
        assertThat(runs.isFinished()).isTrue();
    }

    @Test
    public void shouldBuildProcessRunsWhenJobFinishedDifferentTasks() {
        // Given
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-25T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-25T09:23:30.001Z"),
                        Instant.parse("2022-09-25T09:24:00.001Z")));

        // When
        ProcessRun run1 = ProcessRun.finished("test-task-1", started1, finished1);
        ProcessRun run2 = ProcessRun.finished("test-task-2", started2, finished2);
        ProcessRuns runs = ProcessRuns.latestFirst(Arrays.asList(run2, run1));

        // Then
        assertThat(runs.getRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple("test-task-2", started2, finished2),
                        tuple("test-task-1", started1, finished1));
        assertThat(runs.isStarted()).isTrue();
        assertThat(runs.isFinished()).isTrue();
    }
}
