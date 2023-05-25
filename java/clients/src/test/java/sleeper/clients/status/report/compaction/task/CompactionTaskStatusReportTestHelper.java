/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.stream.Stream;

public class CompactionTaskStatusReportTestHelper {
    private CompactionTaskStatusReportTestHelper() {
    }

    public static CompactionTaskStatus startedTask(String taskId, String startTime) {
        return startedTaskBuilder(taskId, startTime).build();
    }

    public static CompactionTaskStatus startedSplittingTask(String taskId, String startTime) {
        return startedTaskBuilder(taskId, startTime).type(CompactionTaskType.SPLITTING)
                .build();
    }

    private static CompactionTaskStatus.Builder startedTaskBuilder(String taskId, String startTime) {
        return CompactionTaskStatus.builder()
                .startTime(Instant.parse(startTime))
                .taskId(taskId);
    }

    public static CompactionTaskStatus finishedTask(String taskId, String startTime, String finishTime,
                                                    long recordsRead, long linesWritten) {
        return finishedTaskBuilder(taskId, startTime, finishTime, recordsRead, linesWritten).build();
    }

    public static CompactionTaskStatus finishedTask(String taskId, String startTime, String finishTime,
                                                    RecordsProcessedSummary... summaries) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime), CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(summaries)))
                .build();
    }

    public static CompactionTaskStatus finishedSplittingTask(String taskId, String startTime, String finishTime,
                                                             long recordsRead, long linesWritten) {
        return finishedTaskBuilder(taskId, startTime, finishTime, recordsRead, linesWritten)
                .type(CompactionTaskType.SPLITTING).build();
    }

    public static CompactionTaskStatus finishedSplittingTask(String taskId, String startTime, String finishTime,
                                                             RecordsProcessedSummary... summaries) {
        return startedTaskBuilder(taskId, startTime)
                .type(CompactionTaskType.SPLITTING)
                .finished(Instant.parse(finishTime), CompactionTaskFinishedStatus.builder().jobSummaries(Stream.of(summaries)))
                .build();
    }

    private static CompactionTaskStatus.Builder finishedTaskBuilder(String taskId, String startTime,
                                                                    String finishTime, long recordsRead, long linesWritten) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime),
                        taskFinishedStatus(startTime, finishTime, recordsRead, linesWritten));
    }

    private static CompactionTaskFinishedStatus.Builder taskFinishedStatus(
            String startTime, String finishTime, long recordsRead, long linesWritten) {
        return taskFinishedStatusBuilder(startTime, finishTime, recordsRead, linesWritten);
    }

    private static CompactionTaskFinishedStatus.Builder taskFinishedStatusBuilder(
            String startTime, String finishTime, long recordsRead, long linesWritten) {
        return CompactionTaskFinishedStatus.builder()
                .addJobSummary(createSummary(startTime, finishTime, recordsRead, linesWritten));
    }

    private static RecordsProcessedSummary createSummary(
            String startTime, String finishTime, long recordsRead, long linesWritten) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(recordsRead, linesWritten),
                Instant.parse(startTime), Instant.parse(finishTime));
    }
}
