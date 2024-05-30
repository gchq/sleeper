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
package sleeper.ingest.job.status;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

public class IngestJobFailedEvent {
    private final String jobId;
    private final String tableId;
    private final String jobRunId;
    private final String taskId;
    private final Instant finishTime;

    private IngestJobFailedEvent(Builder builder) {
        this.jobId = builder.jobId;
        this.tableId = builder.tableId;
        this.jobRunId = builder.jobRunId;
        this.taskId = builder.taskId;
        this.finishTime = builder.finishTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getFinishTime() {
        return finishTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, jobRunId, taskId, finishTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobFailedEvent)) {
            return false;
        }
        IngestJobFailedEvent other = (IngestJobFailedEvent) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(taskId, other.taskId)
                && Objects.equals(finishTime, other.finishTime);
    }

    @Override
    public String toString() {
        return "IngestJobFailedEvent{jobId=" + jobId + ", tableId=" + tableId + ", jobRunId=" + jobRunId + ", taskId=" + taskId + ", finishTime=" + finishTime + "}";
    }

    public static final class Builder {
        private String jobId;
        private String tableId;
        private String jobRunId;
        private String taskId;
        private Instant finishTime;

        public Builder job(IngestJob job) {
            return jobId(job.getId())
                    .tableId(job.getTableId());
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder finishTime(Instant finishTime) {
            this.finishTime = finishTime;
            return this;
        }

        public IngestJobFailedEvent build() {
            return new IngestJobFailedEvent(this);
        }

    }
}
