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
package sleeper.compaction.job.commit;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobCommitRequest {

    private final CompactionJob job;
    private final String taskId;
    private final String jobRunId;
    private final long recordsWritten;
    private final Instant startTime;
    private final Instant finishTime;

    public CompactionJobCommitRequest(
            CompactionJob job, String taskId, String jobRunId, RecordsProcessedSummary recordsProcessed) {
        this.job = job;
        this.taskId = taskId;
        this.jobRunId = jobRunId;
        this.recordsWritten = recordsProcessed.getRecordsWritten();
        this.startTime = recordsProcessed.getStartTime();
        this.finishTime = recordsProcessed.getFinishTime();
    }

    public CompactionJob getJob() {
        return job;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(job, taskId, jobRunId, recordsWritten, startTime, finishTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompactionJobCommitRequest)) {
            return false;
        }
        CompactionJobCommitRequest other = (CompactionJobCommitRequest) obj;
        return Objects.equals(job, other.job) && Objects.equals(taskId, other.taskId) && Objects.equals(jobRunId, other.jobRunId) && recordsWritten == other.recordsWritten
                && Objects.equals(startTime, other.startTime) && Objects.equals(finishTime, other.finishTime);
    }

    @Override
    public String toString() {
        return "CompactionJobCommitRequest{job=" + job + ", taskId=" + taskId + ", jobRunId=" + jobRunId + ", recordsWritten=" + recordsWritten + ", startTime=" + startTime + ", finishTime="
                + finishTime + "}";
    }

}
