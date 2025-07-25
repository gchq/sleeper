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

package sleeper.clients.report.compaction.task;

import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.time.Instant;
import java.util.stream.Stream;

public class CompactionTaskStatusReportTestHelper {
    private CompactionTaskStatusReportTestHelper() {
    }

    public static CompactionTaskStatus startedTask(String taskId, String startTime) {
        return startedTaskBuilder(taskId, startTime).build();
    }

    private static CompactionTaskStatus.Builder startedTaskBuilder(String taskId, String startTime) {
        return CompactionTaskStatus.builder()
                .startTime(Instant.parse(startTime))
                .taskId(taskId);
    }

    public static CompactionTaskStatus finishedTask(
            String taskId, String startTime, String finishTime, long rowsRead, long rowsWritten) {
        return finishedTaskBuilder(taskId, startTime, finishTime, rowsRead, rowsWritten).build();
    }

    public static CompactionTaskStatus finishedTask(
            String taskId, String startTime, String finishTime, JobRunSummary... summaries) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime), CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(summaries)))
                .build();
    }

    private static CompactionTaskStatus.Builder finishedTaskBuilder(
            String taskId, String startTime, String finishTime, long rowsRead, long rowsWritten) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime),
                        taskFinishedStatus(startTime, finishTime, rowsRead, rowsWritten));
    }

    private static CompactionTaskFinishedStatus.Builder taskFinishedStatus(
            String startTime, String finishTime, long rowsRead, long rowsWritten) {
        return taskFinishedStatusBuilder(startTime, finishTime, rowsRead, rowsWritten);
    }

    private static CompactionTaskFinishedStatus.Builder taskFinishedStatusBuilder(
            String startTime, String finishTime, long rowsRead, long rowsWritten) {
        return CompactionTaskFinishedStatus.builder()
                .addJobSummary(createSummary(startTime, finishTime, rowsRead, rowsWritten));
    }

    private static JobRunSummary createSummary(
            String startTime, String finishTime, long rowsRead, long rowsWritten) {
        return new JobRunSummary(
                new RowsProcessed(rowsRead, rowsWritten),
                Instant.parse(startTime), Instant.parse(finishTime));
    }
}
