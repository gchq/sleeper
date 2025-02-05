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

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.job.status.JobRunFinishedStatus;
import sleeper.core.tracker.job.status.TestJobStartedStatus;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.core.tracker.job.run.JobRunsTestHelper.runsFromUpdates;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.finishedStatus;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.startedStatus;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.TASK_ID_1;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.TASK_ID_2;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.onTask;

public class JobRunsRecordsOutOfOrderTest {

    @Test
    public void shouldReportRunWhenJobFinishedReturnedFromDatabaseOutOfOrder() {
        // Given
        TestJobStartedStatus started = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        JobRunFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 450L, 300L);

        // When
        JobRuns runs = runsFromUpdates(finished, started);

        // Then
        assertThat(runs.getRunsLatestFirst())
                .extracting(JobRun::getStartedStatus, JobRun::getFinishedStatus)
                .containsExactly(
                        tuple(started, finished));
    }

    @Test
    public void shouldReportRunsWhenJobStartedReturnedFromDatabaseOutOfOrder() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));
        TestJobStartedStatus started3 = startedStatus(Instant.parse("2022-09-26T09:23:30.001Z"));

        // When
        JobRuns runs = runsFromUpdates(started3, started1, started2);

        // Then
        assertThat(runs.getRunsLatestFirst())
                .extracting(JobRun::getStartedStatus, JobRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, null),
                        tuple(started2, null),
                        tuple(started1, null));
    }

    @Test
    public void shouldReportRunsWhenLastRunFinishedButReturnedFromDatabaseOutOfOrder() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-25T09:23:30.001Z"));
        TestJobStartedStatus started3 = startedStatus(Instant.parse("2022-09-26T09:23:30.001Z"));
        JobRunFinishedStatus finished = finishedStatus(started3, Duration.ofSeconds(30), 450L, 300L);

        // When
        JobRuns runs = runsFromUpdates(started3, finished, started1, started2);

        // Then
        assertThat(runs.getRunsLatestFirst())
                .extracting(JobRun::getStartedStatus, JobRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, finished),
                        tuple(started2, null),
                        tuple(started1, null));
    }

    @Test
    public void shouldReportRunsOnDifferentTasksWhenJobFinishedFromDatabaseOutOfOrder() {
        // Given
        TestJobStartedStatus started1 = startedStatus(Instant.parse("2022-09-22T09:23:30.001Z"));
        JobRunFinishedStatus finished1 = finishedStatus(started1, Duration.ofSeconds(30), 450L, 300L);
        TestJobStartedStatus started2 = startedStatus(Instant.parse("2022-09-22T09:23:31.001Z"));
        JobRunFinishedStatus finished2 = finishedStatus(started2, Duration.ofSeconds(30), 450L, 300L);

        // When
        JobRuns runs = runsFromUpdates(
                onTask(TASK_ID_1, finished1, started1),
                onTask(TASK_ID_2, finished2, started2));

        // Then
        assertThat(runs.getRunsLatestFirst())
                .extracting(JobRun::getTaskId, JobRun::getStartedStatus, JobRun::getFinishedStatus)
                .containsExactly(
                        tuple(TASK_ID_2, started2, finished2),
                        tuple(TASK_ID_1, started1, finished1));
    }
}
