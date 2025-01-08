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

package sleeper.clients.status.report.ingest.task;

import sleeper.core.tracker.ingest.task.IngestTaskFinishedStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.job.RecordsProcessed;
import sleeper.core.tracker.job.RecordsProcessedSummary;

import java.time.Instant;
import java.util.stream.Stream;

public class IngestTaskStatusReportTestHelper {
    private IngestTaskStatusReportTestHelper() {
    }

    public static IngestTaskStatus startedTask(String taskId, String startTime) {
        return startedTaskBuilder(taskId, startTime).build();
    }

    private static IngestTaskStatus.Builder startedTaskBuilder(String taskId, String startTime) {
        return IngestTaskStatus.builder()
                .startTime(Instant.parse(startTime))
                .taskId(taskId);
    }

    public static IngestTaskStatus finishedTask(
            String taskId, String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return finishedTaskBuilder(taskId, startTime, finishTime, recordsRead, recordsWritten).build();
    }

    public static IngestTaskStatus finishedTask(
            String taskId, String startTime, String finishTime, RecordsProcessedSummary... summaries) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime),
                        IngestTaskFinishedStatus.builder().jobSummaries(Stream.of(summaries)))
                .build();
    }

    private static IngestTaskStatus.Builder finishedTaskBuilder(
            String taskId, String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return startedTaskBuilder(taskId, startTime)
                .finished(Instant.parse(finishTime),
                        taskFinishedStatus(startTime, finishTime, recordsRead, recordsWritten));
    }

    private static IngestTaskFinishedStatus.Builder taskFinishedStatus(
            String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return IngestTaskFinishedStatus.builder()
                .addJobSummary(createSummary(startTime, finishTime, recordsRead, recordsWritten));
    }

    private static RecordsProcessedSummary createSummary(
            String startTime, String finishTime, long recordsRead, long recordsWritten) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(recordsRead, recordsWritten),
                Instant.parse(startTime), Instant.parse(finishTime));
    }
}
