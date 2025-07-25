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

package sleeper.core.tracker.ingest.task;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.job.run.JobRunSummary;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

public class IngestTaskStatusTest {

    @Test
    public void shouldCreateTaskStatus() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");

        // When
        IngestTaskStatus status = startedStatusBuilder(taskStartedTime).build();

        // Then
        assertThat(status).extracting("finishedStatus").isNull();
        assertThat(status.asJobRunReport()).extracting("taskId", "startTime", "finishTime", "finishedSummary")
                .containsExactly("test-task-id", taskStartedTime, null, null);
    }

    @Test
    public void shouldCreateTaskStatusFromFinishedJobList() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:00Z");
        Instant jobStartedTime1 = Instant.parse("2022-09-22T12:00:05Z");
        Instant jobStartedTime2 = Instant.parse("2022-09-22T12:00:20Z");
        Instant jobStartedTime3 = Instant.parse("2022-09-22T12:00:35Z");
        Instant taskFinishedTime = Instant.parse("2022-09-22T12:00:50Z");

        JobRunSummary summary1 = summary(jobStartedTime1, Duration.ofSeconds(10), 1000L, 500L);
        JobRunSummary summary2 = summary(jobStartedTime2, Duration.ofSeconds(10), 1000L, 500L);
        JobRunSummary summary3 = summary(jobStartedTime3, Duration.ofSeconds(10), 1000L, 500L);

        // When
        IngestTaskStatus status = startedStatusBuilder(taskStartedTime)
                .finished(taskFinishedTime, finishedStatusBuilder(summary1, summary2, summary3))
                .build();

        // Then
        assertThat(status).extracting("finishedStatus.totalJobRuns", "finishedStatus.timeSpentOnJobs")
                .containsExactly(3, Duration.ofSeconds(30));
        assertThat(status.asJobRunReport()).extracting("taskId",
                "startTime", "finishTime", "finishedSummary.duration",
                "finishedSummary.rowsRead", "finishedSummary.rowsWritten",
                "finishedSummary.rowsReadPerSecond", "finishedSummary.rowsWrittenPerSecond")
                .containsExactly("test-task-id",
                        taskStartedTime, taskFinishedTime, Duration.ofSeconds(50),
                        3000L, 1500L, 100.0, 50.0);
    }

    private static IngestTaskStatus.Builder startedStatusBuilder(Instant startTime) {
        return IngestTaskStatus.builder().taskId("test-task-id").startTime(startTime);
    }

    private static IngestTaskFinishedStatus.Builder finishedStatusBuilder(JobRunSummary... jobSummaries) {
        return IngestTaskFinishedStatus.builder().jobSummaries(Stream.of(jobSummaries));
    }
}
