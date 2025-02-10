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
package sleeper.core.statestore.transactionlog.transaction;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A transaction to add files to the state store.
 */
public class AddFilesTransaction implements FileReferenceTransaction {

    private final String jobId;
    private final String taskId;
    private final String jobRunId;
    private final Instant writtenTime;
    private final List<AllReferencesToAFile> files;

    public AddFilesTransaction(List<AllReferencesToAFile> files) {
        this(builder().files(files));
    }

    private AddFilesTransaction(Builder builder) {
        jobId = builder.jobId;
        taskId = builder.taskId;
        jobRunId = builder.jobRunId;
        writtenTime = builder.writtenTime;
        files = Objects.requireNonNull(builder.files, "files must not be null")
                .stream().map(file -> file.withCreatedUpdateTime(null)).toList();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (AllReferencesToAFile file : files) {
            if (stateStoreFiles.file(file.getFilename()).isPresent()) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (AllReferencesToAFile file : files) {
            stateStoreFiles.add(StateStoreFile.newFile(updateTime, file));
        }
    }

    /**
     * Reports the files were added against the job tracker. This should be used after the transaction is fully
     * committed to the log.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     */
    public void reportJobCommitted(IngestJobTracker tracker, TableStatus sleeperTable) {
        if (jobId == null) {
            return;
        }
        tracker.jobAddedFiles(IngestJobAddedFilesEvent.builder()
                .jobId(jobId).taskId(taskId).jobRunId(jobRunId)
                .tableId(sleeperTable.getTableUniqueId()).writtenTime(writtenTime)
                .files(files).build());
    }

    /**
     * Reports failure adding files against the job tracker.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     * @param e            the failure
     */
    public void reportJobFailed(IngestJobTracker tracker, TableStatus sleeperTable, Exception e) {
        if (jobId == null) {
            return;
        }
        tracker.jobFailed(IngestJobFailedEvent.builder()
                .jobId(jobId).taskId(taskId).jobRunId(jobRunId)
                .tableId(sleeperTable.getTableUniqueId())
                .failureTime(writtenTime)
                .failure(e)
                .build());
    }

    public List<AllReferencesToAFile> getFiles() {
        return files;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, taskId, jobRunId, writtenTime, files);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AddFilesTransaction)) {
            return false;
        }
        AddFilesTransaction other = (AddFilesTransaction) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(taskId, other.taskId) && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(writtenTime, other.writtenTime)
                && Objects.equals(files, other.files);
    }

    @Override
    public String toString() {
        return "AddFilesTransaction{jobId=" + jobId + ", taskId=" + taskId + ", jobRunId=" + jobRunId + ", writtenTime=" + writtenTime + ", files=" + files + "}";
    }

    /**
     * A builder for this class.
     */
    public static class Builder {
        private String jobId;
        private String taskId;
        private String jobRunId;
        private Instant writtenTime;
        private List<AllReferencesToAFile> files;

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
         * Sets the ID of the task that ran the ingest job.
         *
         * @param  taskId the task ID
         * @return        this builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the time the files were written and ready to be added to the state store.
         *
         * @param  writtenTime the time the files were written
         * @return             this builder
         */
        public Builder writtenTime(Instant writtenTime) {
            this.writtenTime = writtenTime;
            return this;
        }

        /**
         * Sets the ID of the job run that added these files.
         *
         * @param  jobRunId the job run ID
         * @return          this builder
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the files to add to the state store.
         *
         * @param  files the files to add
         * @return       this builder
         */
        public Builder files(List<AllReferencesToAFile> files) {
            this.files = files;
            return this;
        }

        public AddFilesTransaction build() {
            return new AddFilesTransaction(this);
        }
    }
}
