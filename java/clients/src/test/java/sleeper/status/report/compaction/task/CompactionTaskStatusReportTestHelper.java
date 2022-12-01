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

package sleeper.status.report.compaction.task;

import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;

public class CompactionTaskStatusReportTestHelper {
    private CompactionTaskStatusReportTestHelper() {
    }

    public static CompactionTaskStatus startedTask(String taskId, String startTime) {
        return startedTaskBuilder(taskId, startTime).build();
    }

    private static CompactionTaskStatus.Builder startedTaskBuilder(String taskId, String startTime) {
        return CompactionTaskStatus.builder()
                .started(Instant.parse(startTime))
                .taskId(taskId);
    }

    public static CompactionTaskStatus finishedTask(String taskId, String startTime,
                                                    String finishTime, long linesRead, long linesWritten) {
        return finishedTaskBuilder(taskId, startTime, finishTime, linesRead, linesWritten).build();
    }

    private static CompactionTaskStatus.Builder finishedTaskBuilder(String taskId, String startTime,
                                                                    String finishTime, long linesRead, long linesWritten) {
        return CompactionTaskStatus.builder()
                .started(Instant.parse(startTime))
                .finished(CompactionTaskFinishedStatus.builder()
                        .addJobSummary(new RecordsProcessedSummary(
                                new RecordsProcessed(linesRead, linesWritten),
                                Instant.parse(startTime), Instant.parse(finishTime)))
                        .finish(Instant.parse(startTime), Instant.parse(finishTime)), Instant.parse(finishTime))
                .taskId(taskId);
    }
}
