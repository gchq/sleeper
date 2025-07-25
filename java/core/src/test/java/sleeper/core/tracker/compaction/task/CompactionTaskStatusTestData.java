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

package sleeper.core.tracker.compaction.task;

import sleeper.core.tracker.job.run.JobRunSummary;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

/**
 * Generates compaction task statuses for tests.
 */
public class CompactionTaskStatusTestData {
    private CompactionTaskStatusTestData() {
    }

    /**
     * Creates a builder for the status of a compaction task that started. Uses default values for the task ID, the
     * start time.
     *
     * @return the builder
     */
    public static CompactionTaskStatus.Builder startedStatusBuilderWithDefaults() {
        return startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"));
    }

    /**
     * Creates a builder for the status of a compaction task that started. Uses a default value for the task ID.
     *
     * @param  startTime the time the task started
     * @return           the builder
     */
    public static CompactionTaskStatus.Builder startedStatusBuilder(Instant startTime) {
        return CompactionTaskStatus.builder().taskId("test-task-id").startTime(startTime);
    }

    /**
     * Creates the status of a compaction task that finished. Uses default values.
     *
     * @return the status
     */
    public static CompactionTaskStatus finishedStatusWithDefaults() {
        return finishedStatus("test-task-id",
                Instant.parse("2023-03-30T11:44:00Z"),
                Instant.parse("2023-03-30T12:00:00Z"),
                summary(Instant.parse("2023-03-30T11:44:00Z"), Duration.ofMinutes(5),
                        100L, 100L));
    }

    /**
     * Creates the status of a compaction task that finished.
     *
     * @param  taskId     the task ID
     * @param  startTime  the time the task started
     * @param  finishTime the time the task finished
     * @return            the status
     */
    public static CompactionTaskStatus finishedStatusWithDefaultSummary(
            String taskId, Instant startTime, Instant finishTime) {
        return finishedStatus(taskId, startTime, finishTime, summary(startTime, Duration.ofMinutes(5), 100L, 100L));
    }

    /**
     * Creates the status of a compaction task that finished.
     *
     * @param  taskId     the task ID
     * @param  startTime  the time the task started
     * @param  finishTime the time the task finished
     * @param  summary    summaries of job runs that happened in the task
     * @return            the status
     */
    public static CompactionTaskStatus finishedStatus(
            String taskId, Instant startTime, Instant finishTime, JobRunSummary... summary) {
        return startedStatusBuilder(startTime).taskId(taskId)
                .finishedStatus(finishedStatusBuilder(summary).finish(finishTime).build())
                .build();
    }

    /**
     * Creates a builder for a status update for when a compaction task finished. Populates the status update with
     * information about the job runs that happened in the task.
     *
     * @param  jobSummaries summaries of job runs that happened in the task
     * @return              the builder
     */
    public static CompactionTaskFinishedStatus.Builder finishedStatusBuilder(JobRunSummary... jobSummaries) {
        return CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(jobSummaries));
    }
}
