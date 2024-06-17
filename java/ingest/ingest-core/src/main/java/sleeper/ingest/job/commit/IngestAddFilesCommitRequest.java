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
package sleeper.ingest.job.commit;

import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;

import java.util.List;
import java.util.Objects;

/**
 * A request to commit files to the state store that have been written during an ingest or bulk import.
 */
public class IngestAddFilesCommitRequest {
    private final IngestJob ingestJob;
    private final String tableId;
    private final String taskId;
    private final String jobRunId;
    private final List<FileReference> fileReferences;

    private IngestAddFilesCommitRequest(Builder builder) {
        this.ingestJob = builder.ingestJob;
        this.tableId = Objects.requireNonNull(ingestJob == null ? builder.tableId : ingestJob.getTableId(), "tableId must not be null");
        this.taskId = builder.taskId;
        this.jobRunId = builder.jobRunId;
        this.fileReferences = Objects.requireNonNull(builder.fileReferences, "fileReferences must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public IngestJob getJob() {
        return ingestJob;
    }

    public String getTableId() {
        return tableId;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<FileReference> getFileReferences() {
        return fileReferences;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestJob, taskId, jobRunId, fileReferences);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestAddFilesCommitRequest)) {
            return false;
        }
        IngestAddFilesCommitRequest other = (IngestAddFilesCommitRequest) obj;
        return Objects.equals(ingestJob, other.ingestJob) && Objects.equals(taskId, other.taskId)
                && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(fileReferences, other.fileReferences);
    }

    @Override
    public String toString() {
        return "IngestAddFilesCommitRequest{ingestJob=" + ingestJob + ", taskId=" + taskId
                + ", jobRunId=" + jobRunId + ", fileReferences=" + fileReferences + "}";
    }

    /**
     * Builder for add files commit requests.
     */
    public static class Builder {
        private IngestJob ingestJob;
        private String tableId;
        private String taskId;
        private String jobRunId;
        private List<FileReference> fileReferences;

        /**
         * Sets the ingest job.
         *
         * @param  ingestJob the ingest job
         * @return           the builder for chaining
         */
        public Builder ingestJob(IngestJob ingestJob) {
            this.ingestJob = ingestJob;
            return this;
        }

        /**
         * Sets the Sleeper table ID.
         *
         * @param  tableId the table ID
         * @return         the builder for chaining
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the ingest task ID.
         *
         * @param  taskId the ingest task ID
         * @return        the builder for chaining
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
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
         * Sets the file references to be committed.
         *
         * @param  fileReferences the list of file references
         * @return                the builder for chaining
         */
        public Builder fileReferences(List<FileReference> fileReferences) {
            this.fileReferences = fileReferences;
            return this;
        }

        public IngestAddFilesCommitRequest build() {
            return new IngestAddFilesCommitRequest(this);
        }
    }
}
