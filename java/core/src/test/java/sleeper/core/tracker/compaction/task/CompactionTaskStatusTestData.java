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
 * This class is used to generate test data for CompactionTaskStatuses in different states.
 */
public class CompactionTaskStatusTestData {
    private CompactionTaskStatusTestData() {
    }

    /**
     * This method starts the default StatusBuilder with taskId 'test-task-id' at the StartTime '2023-03-30T11:44:00Z'.
     *
     * @return CompactionTaskStatus Builder of the started task.
     */
    public static CompactionTaskStatus.Builder startedStatusBuilderWithDefaults() {
        return startedStatusBuilder(Instant.parse("2023-03-30T11:44:00Z"));
    }

    /**
     * This method starts the default StatusBuilder with taskId 'test-task-id' at the provided start time.
     *
     * @param  startTime Instant that the task should be started from.
     * @return           CompactionTaskStatus Builder of the started task.
     */
    public static CompactionTaskStatus.Builder startedStatusBuilder(Instant startTime) {
        return CompactionTaskStatus.builder().taskId("test-task-id").startTime(startTime);
    }

    /**
     * This method returns a CompactionTaskStatus for a finished task with default settings.
     * Default settings are taskId: test-task-id.
     * startTime: 2023-03-30T11:44:00Z.
     * finishTime: 2023-03-30T12:00:00Z.
     * JobRunSummay: startTime, 5 minute duration, 100L lines read and 100L records written.
     *
     * @return CompactionTaskStatus
     */
    public static CompactionTaskStatus finishedStatusWithDefaults() {
        return finishedStatus("test-task-id",
                Instant.parse("2023-03-30T11:44:00Z"),
                Instant.parse("2023-03-30T12:00:00Z"),
                summary(Instant.parse("2023-03-30T11:44:00Z"), Duration.ofMinutes(5),
                        100L, 100L));
    }

    /**
     * This method returns a CompactionTaskStatus for a finished task with default summary.
     * Default JobRunSummay: startTime, 5 minute duration, 100L lines read and 100L records written.
     *
     * @param  taskId     String of the taskId for the finished task.
     * @param  startTime  Instant of the start time for the finished task.
     * @param  finishTime Instant of the finish time for the finished task.
     * @return            CompactionTaskStatus
     */
    public static CompactionTaskStatus finishedStatusWithDefaultSummary(
            String taskId, Instant startTime, Instant finishTime) {
        return finishedStatus(taskId, startTime, finishTime, summary(startTime, Duration.ofMinutes(5), 100L, 100L));
    }

    /**
     * This method returns a CompactionTaskStatus for a finished task with provided details.
     *
     * @param  taskId     String of the taskId for the finished task.
     * @param  startTime  Instant of the start time for the finished task.
     * @param  finishTime Instant of the finish time for the finished task.
     * @param  summary    Optional JobRunSummary of the task that has finished.
     * @return            CompactionTaskStatus
     */
    public static CompactionTaskStatus finishedStatus(
            String taskId, Instant startTime, Instant finishTime, JobRunSummary... summary) {
        return startedStatusBuilder(startTime).taskId(taskId)
                .finishedStatus(finishedStatusBuilder(summary).finish(finishTime).build())
                .build();
    }

    /**
     * This method takes a JobRunSummary and returns a builder with the job summarised.
     *
     * @param  jobSummaries Optional JobRunSummary of the task that has finished to be summarised.
     * @return              CompactionTaskFinishedStatus Builder
     */
    public static CompactionTaskFinishedStatus.Builder finishedStatusBuilder(JobRunSummary... jobSummaries) {
        return CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(jobSummaries));
    }
}
