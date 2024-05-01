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
package sleeper.compaction.job;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;

public class CompactionJobRunCompleted {

    private final CompactionJob job;
    private final String taskId;
    private final Instant startTime;
    private final Instant finishTime;
    private final long recordsRead;
    private final long recordsWritten;

    public CompactionJobRunCompleted(
            CompactionJob job, String taskId, RecordsProcessedSummary recordsProcessed) {
        this.job = job;
        this.taskId = taskId;
        this.startTime = recordsProcessed.getStartTime();
        this.finishTime = recordsProcessed.getFinishTime();
        this.recordsRead = recordsProcessed.getRecordsRead();
        this.recordsWritten = recordsProcessed.getRecordsWritten();
    }

    public CompactionJob getJob() {
        return job;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public RecordsProcessed buildRecordsProcessed() {
        return new RecordsProcessed(recordsRead, recordsWritten);
    }

    public RecordsProcessedSummary buildRecordsProcessedSummary() {
        return new RecordsProcessedSummary(buildRecordsProcessed(), startTime, finishTime);
    }

}
