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
package sleeper.compaction.job.completion;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobCompletionRequest {

    private final CompactionJob job;
    private final String taskId;
    private final Instant startTime;
    private final Instant finishTime;
    private final long recordsRead;
    private final long recordsWritten;

    public CompactionJobCompletionRequest(
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

    @Override
    public int hashCode() {
        return Objects.hash(job, taskId, startTime, finishTime, recordsRead, recordsWritten);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobCompletionRequest)) {
            return false;
        }
        CompactionJobCompletionRequest other = (CompactionJobCompletionRequest) obj;
        return Objects.equals(job, other.job) && Objects.equals(taskId, other.taskId) && Objects.equals(startTime, other.startTime) && Objects.equals(finishTime, other.finishTime)
                && recordsRead == other.recordsRead && recordsWritten == other.recordsWritten;
    }

    @Override
    public String toString() {
        return "CompactionJobCompletionRequest{job=" + job + ", taskId=" + taskId + ", startTime=" + startTime + ", finishTime=" + finishTime + ", recordsRead=" + recordsRead + ", recordsWritten="
                + recordsWritten + "}";
    }

}
