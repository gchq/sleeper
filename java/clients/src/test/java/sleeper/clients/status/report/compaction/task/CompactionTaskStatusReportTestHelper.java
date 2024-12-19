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

package sleeper.clients.status.report.compaction.task;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;

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
            String taskId, String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return finishedTaskBuilder(taskId, startTime, finishTime, recordsRead, recordsWritten).build();
    }

    public static CompactionTaskStatus finishedTask(
            String taskId, String startTime, String finishTime, RecordsProcessedSummary... summaries) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime), CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(summaries)))
                .build();
    }

    private static CompactionTaskStatus.Builder finishedTaskBuilder(
            String taskId, String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime),
                        taskFinishedStatus(startTime, finishTime, recordsRead, recordsWritten));
    }

    private static CompactionTaskFinishedStatus.Builder taskFinishedStatus(
            String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return taskFinishedStatusBuilder(startTime, finishTime, recordsRead, recordsWritten);
    }

    private static CompactionTaskFinishedStatus.Builder taskFinishedStatusBuilder(
            String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return CompactionTaskFinishedStatus.builder()
                .addJobSummary(createSummary(startTime, finishTime, recordsRead, recordsWritten));
    }

    private static RecordsProcessedSummary createSummary(
            String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(recordsRead, recordsWritten),
                Instant.parse(startTime), Instant.parse(finishTime));
    }
}
