/*
 * Copyright 2022-2025 Crown Copyright
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

import java.time.Instant;
import java.util.Objects;

/**
 * An event for when an ingest job was started. Used in the ingest job tracker.
 */
public class IngestJobStartedEvent implements IngestJobEvent {
    private final String jobId;
    private final String tableId;
    private final String jobRunId;
    private final String taskId;
    private final int fileCount;
    private final Instant startTime;

    private IngestJobStartedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        jobRunId = Objects.requireNonNull(builder.jobRunId, "jobRunId must not be null");
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        fileCount = builder.fileCount;
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
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

    public int getFileCount() {
        return fileCount;
    }

    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        IngestJobStartedEvent that = (IngestJobStartedEvent) object;
        return fileCount == that.fileCount && Objects.equals(jobId, that.jobId)
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(jobRunId, that.jobRunId) && Objects.equals(taskId, that.taskId)
                && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, fileCount, jobRunId, taskId, startTime);
    }

    @Override
    public String toString() {
        return "IngestJobStartedEvent{" +
                "jobId='" + jobId + '\'' +
                ", tableId='" + tableId + '\'' +
                ", fileCount=" + fileCount +
                ", jobRunId='" + jobRunId + '\'' +
                ", taskId='" + taskId + '\'' +
                ", startTime=" + startTime +
                '}';
    }

    /**
     * Builder for ingest job started event objects.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private int fileCount;
        private String jobRunId;
        private String taskId;
        private Instant startTime;

        private Builder() {
        }

        /**
         * Sets the IDs relating to this run of the job.
         *
         * @param  jobRunIds the IDs
         * @return           the builder
         */
        public Builder jobRunIds(IngestJobRunIds jobRunIds) {
            return jobId(jobRunIds.getJobId())
                    .tableId(jobRunIds.getTableId())
                    .jobRunId(jobRunIds.getJobRunId())
                    .taskId(jobRunIds.getTaskId());
        }

        /**
         * Sets the ingest job ID.
         *
         * @param  jobId the ingest job ID
         * @return       the builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the table ID.
         *
         * @param  tableId the table ID
         * @return         the builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the input file count.
         *
         * @param  fileCount the input file count
         * @return           the builder
         */
        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        /**
         * Sets the job run ID.
         *
         * @param  jobRunId the job run ID
         * @return          the builder
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the task ID.
         *
         * @param  taskId the task ID
         * @return        the builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the start time.
         *
         * @param  startTime the start time
         * @return           the builder
         */
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public IngestJobStartedEvent build() {
            return new IngestJobStartedEvent(this);
        }
    }
}
