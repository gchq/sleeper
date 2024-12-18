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

package sleeper.compaction.core.task;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;

public class CompactionTaskStatusTestData {
    private CompactionTaskStatusTestData() {
    }

    public static CompactionTaskStatus.Builder startedStatusBuilderWithDefaults() {
        return startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"));
    }

    public static CompactionTaskStatus.Builder startedStatusBuilder(Instant startTime) {
        return CompactionTaskStatus.builder().taskId("test-task-id").startTime(startTime);
    }

    public static CompactionTaskStatus finishedStatusWithDefaults() {
        return finishedStatus("test-task-id",
                Instant.parse("2023-03-30T11:44:00Z"),
                Instant.parse("2023-03-30T12:00:00Z"),
                summary(Instant.parse("2023-03-30T11:44:00Z"), Duration.ofMinutes(5),
                        100L, 100L));
    }

    public static CompactionTaskStatus finishedStatusWithDefaultSummary(
            String taskId, Instant startTime, Instant finishTime) {
        return finishedStatus(taskId, startTime, finishTime, summary(startTime, Duration.ofMinutes(5), 100L, 100L));
    }

    public static CompactionTaskStatus finishedStatus(
            String taskId, Instant startTime, Instant finishTime, RecordsProcessedSummary... summary) {
        return startedStatusBuilder(startTime).taskId(taskId)
                .finishedStatus(finishedStatusBuilder(summary).finish(finishTime).build())
                .build();
    }

    public static CompactionTaskFinishedStatus.Builder finishedStatusBuilder(RecordsProcessedSummary... jobSummaries) {
        return CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(jobSummaries));
    }
}
