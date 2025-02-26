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
package sleeper.core.tracker.ingest.job.update;

import java.util.Objects;

/**
 * The IDs required for a run of an ingest job in the ingest job tracker.
 */
public class IngestJobRunIds {
    private final String jobId;
    private final String tableId;
    private final String jobRunId;
    private final String taskId;

    private IngestJobRunIds(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        jobRunId = Objects.requireNonNull(builder.jobRunId, "jobRunId must not be null");
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
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

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, jobRunId, taskId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobRunIds)) {
            return false;
        }
        IngestJobRunIds other = (IngestJobRunIds) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(taskId, other.taskId);
    }

    @Override
    public String toString() {
        return "IngestJobRunIds{jobId=" + jobId + ", tableId=" + tableId + ", jobRunId=" + jobRunId + ", taskId=" + taskId + "}";
    }

    /**
     * Builder for this class.
     */
    public static class Builder {
        private String jobId;
        private String tableId;
        private String jobRunId;
        private String taskId;

        private Builder() {
        }

        /**
         * Sets the ingest job ID.
         *
         * @param  jobId the job ID
         * @return       this builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the Sleeper table ID.
         *
         * @param  jobId the Sleeper table ID
         * @return       this builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the correlation ID of the run of the job.
         *
         * @param  jobRunId the run ID
         * @return          this builder
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the ID of the task that ran the job.
         *
         * @param  taskId the task ID
         * @return        this builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public IngestJobRunIds build() {
            return new IngestJobRunIds(this);
        }

    }

}
