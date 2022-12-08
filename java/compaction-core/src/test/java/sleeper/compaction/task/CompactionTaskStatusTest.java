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

package sleeper.compaction.task;

import org.junit.Test;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionTaskStatusTest {

    @Test
    public void shouldCreateCompactionTaskStatus() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");

        // When
        CompactionTaskStatus status = startedStatusBuilder(taskStartedTime).build();

        // Then
        assertThat(status).extracting("startedStatus.startTime")
                .isEqualTo(taskStartedTime);
    }

    @Test
    public void shouldCreateCompactionTaskStatusFromFinishedStandardJobList() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");
        Instant jobFinishTime3 = Instant.parse("2022-09-22T16:00:14.000Z");

        // When
        CompactionTaskStatus.Builder taskStatusBuilder = startedStatusBuilder(taskStartedTime);
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        createJobSummaries().forEach(taskFinishedBuilder::addJobSummary);
        taskStatusBuilder.finished(taskFinishedBuilder, jobFinishTime3.toEpochMilli());

        // Then
        assertThat(taskStatusBuilder.build()).extracting("startedStatus.startTime", "finishedStatus.finishTime",
                        "finishedStatus.totalJobs", "finishedStatus.totalRuntimeInSeconds", "finishedStatus.totalRecordsRead",
                        "finishedStatus.totalRecordsWritten", "finishedStatus.recordsReadPerSecond", "finishedStatus.recordsWrittenPerSecond")
                .containsExactly(taskStartedTime, jobFinishTime3, 3, 14400.0, 14400L, 7200L, 1.0, 0.5);
    }

    @Test
    public void shouldCreateCompactionTaskStatusFromFinishedSplittingJobList() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");
        Instant jobFinishTime3 = Instant.parse("2022-09-22T16:00:14.000Z");

        // When
        CompactionTaskStatus.Builder taskStatusBuilder = startedStatusBuilder(taskStartedTime);
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        createJobSummaries().forEach(taskFinishedBuilder::addJobSummary);
        taskStatusBuilder.finished(taskFinishedBuilder, jobFinishTime3.toEpochMilli());

        // Then
        assertThat(taskStatusBuilder.build()).extracting("startedStatus.startTime", "finishedStatus.finishTime",
                        "finishedStatus.totalJobs", "finishedStatus.totalRuntimeInSeconds", "finishedStatus.totalRecordsRead",
                        "finishedStatus.totalRecordsWritten", "finishedStatus.recordsReadPerSecond", "finishedStatus.recordsWrittenPerSecond")
                .containsExactly(taskStartedTime, jobFinishTime3, 3, 14400.0, 14400L, 7200L, 1.0, 0.5);
    }

    private static CompactionTaskStatus.Builder startedStatusBuilder(Instant startTime) {
        return CompactionTaskStatus.builder().taskId("test-task-id").started(startTime);
    }

    private static List<RecordsProcessedSummary> createJobSummaries() {
        Instant jobStartedUpdateTime1 = Instant.parse("2022-09-22T14:00:04.000Z");
        Instant jobFinishTime1 = Instant.parse("2022-09-22T14:00:14.000Z");

        Instant jobStartedUpdateTime2 = Instant.parse("2022-09-22T15:00:04.000Z");
        Instant jobFinishTime2 = Instant.parse("2022-09-22T15:00:14.000Z");

        Instant jobStartedUpdateTime3 = Instant.parse("2022-09-22T16:00:04.000Z");
        Instant jobFinishTime3 = Instant.parse("2022-09-22T16:00:14.000Z");

        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(4800L, 2400L), jobStartedUpdateTime1, jobFinishTime1);
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(4800L, 2400L), jobStartedUpdateTime2, jobFinishTime2);
        RecordsProcessedSummary summary3 = new RecordsProcessedSummary(
                new RecordsProcessed(4800L, 2400L), jobStartedUpdateTime3, jobFinishTime3);

        return Arrays.asList(summary1, summary2, summary3);
    }
}
