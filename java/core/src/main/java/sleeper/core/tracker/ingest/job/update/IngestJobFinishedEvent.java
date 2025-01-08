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

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.JobRunSummary;

import java.util.List;
import java.util.Objects;

/**
 * An event for when an ingest job was finished. Used in the ingest job tracker.
 */
public class IngestJobFinishedEvent implements IngestJobEvent {
    private final String jobId;
    private final String tableId;
    private final JobRunSummary summary;
    private final int numFilesWrittenByJob;
    private final boolean committedBySeparateFileUpdates;
    private final String jobRunId;
    private final String taskId;

    private IngestJobFinishedEvent(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        summary = Objects.requireNonNull(builder.summary, "summary must not be null");
        numFilesWrittenByJob = Objects.requireNonNull(builder.numFilesWrittenByJob, "numFilesWrittenByJob must not be null");
        committedBySeparateFileUpdates = builder.committedBySeparateFileUpdates;
        jobRunId = builder.jobRunId;
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

    public JobRunSummary getSummary() {
        return summary;
    }

    public int getNumFilesWrittenByJob() {
        return numFilesWrittenByJob;
    }

    public boolean isCommittedBySeparateFileUpdates() {
        return committedBySeparateFileUpdates;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId, summary, numFilesWrittenByJob, committedBySeparateFileUpdates, jobRunId, taskId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobFinishedEvent)) {
            return false;
        }
        IngestJobFinishedEvent other = (IngestJobFinishedEvent) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(tableId, other.tableId) && Objects.equals(summary, other.summary)
                && Objects.equals(numFilesWrittenByJob, other.numFilesWrittenByJob)
                && committedBySeparateFileUpdates == other.committedBySeparateFileUpdates && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(taskId, other.taskId);
    }

    @Override
    public String toString() {
        return "IngestJobFinishedEvent{jobId=" + jobId + ", tableId=" + tableId + ", summary=" + summary + ", numFilesWrittenByJob=" + numFilesWrittenByJob + ", committedBySeparateFileUpdates="
                + committedBySeparateFileUpdates + ", jobRunId=" + jobRunId + ", taskId=" + taskId + "}";
    }

    /**
     * Builder for ingest job finished event objects.
     */
    public static final class Builder {
        private String jobId;
        private String tableId;
        private JobRunSummary summary;
        private Integer numFilesWrittenByJob;
        private boolean committedBySeparateFileUpdates;
        private String jobRunId;
        private String taskId;

        private Builder() {
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
         * Sets the records processed summary.
         *
         * @param  summary the records processed summary
         * @return         the builder
         */
        public Builder summary(JobRunSummary summary) {
            this.summary = summary;
            return this;
        }

        /**
         * Sets the number of files written during the job.
         *
         * @param  numFilesWrittenByJob the number of files
         * @return                      the builder
         */
        public Builder numFilesWrittenByJob(int numFilesWrittenByJob) {
            this.numFilesWrittenByJob = numFilesWrittenByJob;
            return this;
        }

        /**
         * Sets the files written during the job.
         *
         * @param  files the files
         * @return       the builder
         */
        public Builder filesWrittenByJob(List<AllReferencesToAFile> files) {
            return numFilesWrittenByJob(files.size());
        }

        /**
         * Sets the file references added for all files written during the job.
         *
         * @param  fileReferences the file references
         * @return                the builder
         */
        public Builder fileReferencesAddedByJob(List<FileReference> fileReferences) {
            return filesWrittenByJob(AllReferencesToAFile.newFilesWithReferences(fileReferences));
        }

        /**
         * Sets whether or not separate status updates are used for files added to the state store. If true, the job
         * will only be committed when all files have been added.
         *
         * @param  committedBySeparateFileUpdates true if the job is committed by separate updates to add files
         * @return                                the builder
         */
        public Builder committedBySeparateFileUpdates(boolean committedBySeparateFileUpdates) {
            this.committedBySeparateFileUpdates = committedBySeparateFileUpdates;
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

        public IngestJobFinishedEvent build() {
            return new IngestJobFinishedEvent(this);
        }
    }
}
