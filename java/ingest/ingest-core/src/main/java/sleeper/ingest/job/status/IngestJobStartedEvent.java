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

/**
 * An event for when an ingest job was started. Used in the ingest job status store.
 */
public class IngestJobStartedEvent {
    private final String jobId;
    private final String tableId;
    private final int fileCount;
    private final String jobRunId;
    private final String taskId;
    private final Instant startTime;
    private final boolean startOfRun;

    private IngestJobStartedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        fileCount = builder.fileCount;
        jobRunId = builder.jobRunId;
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        startTime = Objects.requireNonNull(builder.startTime, "startTime must not be null");
        startOfRun = builder.startOfRun;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an instance of this class. This constructor is specifically for ingest jobs and creates an event that
     * marks the start of a job run. This is not used for bulk import jobs, as they have a validation event before
     * this, and this validation event marks the start of a job run. Bulk import jobs should use the
     * {@link IngestJobStartedEvent#validatedIngestJobStarted} constructor.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           an instance of this class
     */
    public static Builder ingestJobStarted(IngestJob job, Instant startTime) {
        return builder()
                .job(job)
                .startTime(startTime)
                .startOfRun(true);
    }

    /**
     * Creates a builder for this class. This constructor is specifically for bulk import jobs and creates an event
     * that indicates the job has been picked up in the Spark cluster by the driver.
     * <p>
     * Note that this does not mark the start of a job run. Once the bulk import starter picks up a bulk import job, it
     * validates the job and saves an event then, which marks the start of a job run.
     * <p>
     * This is not used for ingest jobs. Ingest jobs should use the {@link IngestJobStartedEvent#ingestJobStarted}
     * constructor.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           a builder for this class
     */
    public static Builder validatedIngestJobStarted(IngestJob job, Instant startTime) {
        return builder()
                .job(job)
                .startTime(startTime)
                .startOfRun(false);
    }

    public String getJobId() {
        return jobId;
    }

    public String getTableId() {
        return tableId;
    }

    public int getFileCount() {
        return fileCount;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public boolean isStartOfRun() {
        return startOfRun;
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
        return fileCount == that.fileCount && startOfRun == that.startOfRun && Objects.equals(jobId, that.jobId)
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(jobRunId, that.jobRunId) && Objects.equals(taskId, that.taskId)
                && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, fileCount, jobRunId, taskId, startTime, startOfRun);
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
                ", startOfRun=" + startOfRun +
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
        private boolean startOfRun;

        private Builder() {
        }

        /**
         * Sets the ingest job ID, table ID, and file count using the provided ingest job.
         *
         * @param  job the ingest job
         * @return     the builder
         */
        public Builder job(IngestJob job) {
            return jobId(job.getId())
                    .tableId(job.getTableId())
                    .fileCount(job.getFileCount());
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

        /**
         * Sets whether this event marks the start of a job run.
         *
         * @param  startOfRun whether this event marks the start of a job run
         * @return            the builder
         */
        public Builder startOfRun(boolean startOfRun) {
            this.startOfRun = startOfRun;
            return this;
        }

        public IngestJobStartedEvent build() {
            return new IngestJobStartedEvent(this);
        }
    }
}
