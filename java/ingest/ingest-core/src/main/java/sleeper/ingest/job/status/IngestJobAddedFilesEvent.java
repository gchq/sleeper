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

import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * An event for when an ingest job committed files to the state store. Used in the ingest job status store.
 */
public class IngestJobAddedFilesEvent {
    private final String jobId;
    private final String tableId;
    private final String jobRunId;
    private final String taskId;
    private final Instant writtenTime;
    private final int fileCount;

    private IngestJobAddedFilesEvent(Builder builder) {
        this.jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        this.tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        this.jobRunId = builder.jobRunId;
        this.taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        this.writtenTime = Objects.requireNonNull(builder.writtenTime, "writtenTime must not be null");
        this.fileCount = builder.fileCount;
    }

    /**
     * Creates a builder for when an ingest job committed files to the state store.
     *
     * @param  job            the job
     * @param  fileReferences the file references added to the state store
     * @param  writtenTime    the time the files were written
     * @return                a builder to set further information
     */
    public static Builder ingestJobAddedFiles(IngestJob job, List<FileReference> fileReferences, Instant writtenTime) {
        return builder()
                .job(job)
                .writtenTime(writtenTime)
                .fileReferences(fileReferences);
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

    public Instant getWrittenTime() {
        return writtenTime;
    }

    public int getFileCount() {
        return fileCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, jobRunId, taskId, writtenTime, fileCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobAddedFilesEvent)) {
            return false;
        }
        IngestJobAddedFilesEvent other = (IngestJobAddedFilesEvent) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(taskId, other.taskId)
                && Objects.equals(writtenTime, other.writtenTime) && fileCount == other.fileCount;
    }

    @Override
    public String toString() {
        return "IngestJobAddedFilesEvent{jobId=" + jobId + ", tableId=" + tableId + ", jobRunId=" + jobRunId + ", taskId=" + taskId + ", writtenTime=" + writtenTime + ", fileCount=" + fileCount + "}";
    }

    /**
     * Builder to create an event when an ingest job committed files to the state store.
     */
    public static class Builder {
        private String jobId;
        private String tableId;
        private String jobRunId;
        private String taskId;
        private Instant writtenTime;
        private int fileCount;

        /**
         * Sets the job ID and table ID from the given ingest job.
         *
         * @param  job the job
         * @return     the builder for chaining
         */
        public Builder job(IngestJob job) {
            return jobId(job.getId())
                    .tableId(job.getTableId());
        }

        /**
         * Sets the job ID.
         *
         * @param  jobId the job ID
         * @return       the builder for chaining
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the table ID.
         *
         * @param  tableId the table ID
         * @return         the builder for chaining
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the job run ID.
         *
         * @param  jobRunId the job run ID
         * @return          the builder for chaining
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the task ID.
         *
         * @param  taskId the task ID
         * @return        the builder for chaining
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the written time.
         *
         * @param  writtenTime the time the files were written
         * @return             the builder for chaining
         */
        public Builder writtenTime(Instant writtenTime) {
            this.writtenTime = writtenTime;
            return this;
        }

        /**
         * Sets the added file references.
         *
         * @param  fileReferences the file references that were added
         * @return                the builder for chaining
         */
        public Builder fileReferences(List<FileReference> fileReferences) {
            this.fileCount = fileReferences.size();
            return this;
        }

        public IngestJobAddedFilesEvent build() {
            return new IngestJobAddedFilesEvent(this);
        }
    }
}
