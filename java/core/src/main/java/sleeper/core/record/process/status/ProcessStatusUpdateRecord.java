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
package sleeper.core.record.process.status;

import java.time.Instant;
import java.util.Objects;

public class ProcessStatusUpdateRecord {

    private final String jobId;
    private final ProcessStatusUpdate statusUpdate;
    private final String jobRunId;
    private final String taskId;
    private final Instant expiryDate;

    private ProcessStatusUpdateRecord(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        statusUpdate = Objects.requireNonNull(builder.statusUpdate, "statusUpdate must not be null");
        jobRunId = builder.jobRunId;
        taskId = builder.taskId;
        expiryDate = builder.expiryDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public ProcessStatusUpdate getStatusUpdate() {
        return statusUpdate;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getUpdateTime() {
        return statusUpdate.getUpdateTime();
    }

    public Instant getExpiryDate() {
        return expiryDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcessStatusUpdateRecord that = (ProcessStatusUpdateRecord) o;

        if (!jobId.equals(that.jobId)) {
            return false;
        }
        if (!statusUpdate.equals(that.statusUpdate)) {
            return false;
        }
        if (!Objects.equals(jobRunId, that.jobRunId)) {
            return false;
        }
        if (!Objects.equals(taskId, that.taskId)) {
            return false;
        }
        return Objects.equals(expiryDate, that.expiryDate);
    }

    @Override
    public int hashCode() {
        int result = jobId.hashCode();
        result = 31 * result + statusUpdate.hashCode();
        result = 31 * result + (jobRunId != null ? jobRunId.hashCode() : 0);
        result = 31 * result + (taskId != null ? taskId.hashCode() : 0);
        result = 31 * result + (expiryDate != null ? expiryDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ProcessStatusUpdateRecord{" +
                "jobId='" + jobId + '\'' +
                ", statusUpdate=" + statusUpdate +
                ", jobRunId='" + jobRunId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", expiryDate=" + expiryDate +
                '}';
    }

    public static final class Builder {
        private String jobId;
        private ProcessStatusUpdate statusUpdate;
        private String jobRunId;
        private String taskId;
        private Instant expiryDate;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder statusUpdate(ProcessStatusUpdate statusUpdate) {
            this.statusUpdate = statusUpdate;
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

        public Builder expiryDate(Instant expiryDate) {
            this.expiryDate = expiryDate;
            return this;
        }

        public ProcessStatusUpdateRecord build() {
            return new ProcessStatusUpdateRecord(this);
        }
    }
}
