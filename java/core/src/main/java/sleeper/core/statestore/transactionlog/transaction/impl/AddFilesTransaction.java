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
package sleeper.core.statestore.transactionlog.transaction.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Adds files to the Sleeper table, with any number of references. Each new file should be specified once, with all
 * its references. Once a file has been added, it may not be added again, even as a reference on a different partition.
 * <p>
 * A file must never be referenced in two partitions where one is a descendent of another. This means each record in
 * a file must only be covered by one reference. A partition covers a range of records. A partition which is the
 * child of another covers a sub-range within the parent partition.
 */
public class AddFilesTransaction implements FileReferenceTransaction {

    public static final Logger LOGGER = LoggerFactory.getLogger(AddFilesTransaction.class);

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

    /**
     * Creates a transaction to add files with the given references.
     *
     * @param  fileReferences the file references
     * @return                the transaction
     */
    public static AddFilesTransaction fromReferences(List<FileReference> fileReferences) {
        return builder().fileReferences(fileReferences).build();
    }

    /**
     * Commit this transaction directly to the state store without going to the commit queue. This will throw any
     * validation exceptions immediately, even if they wouldn't be as part of an asynchronous commit.
     *
     * @param  stateStore                 the state store
     * @throws FileAlreadyExistsException if a file already exists
     * @throws StateStoreException        if the update fails for another reason
     */
    public void synchronousCommit(StateStore stateStore) throws StateStoreException {
        stateStore.addTransaction(AddTransactionRequest.withTransaction(this)
                .beforeApplyListener(StateListenerBeforeApply.withFilesState(state -> validateFiles(state)))
                .build());
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        // We want to update the job tracker whether the new files are valid or not, and the job tracker is updated
        // based on the transaction log. This means we still want to add the transaction to the log if it's invalid.
        // We discard any invalid files at the point when we apply the transaction in the apply method.
    }

    /**
     * Validates whether the files should actually be added. This is because the transaction will be added to the log
     * regardless of whether the files may be added, so that any failure can be reported to the job tracker after the
     * fact.
     *
     * @param  stateStoreFiles            the state before the transaction
     * @throws FileAlreadyExistsException if a file already exists
     */
    public void validateFiles(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (AllReferencesToAFile file : files) {
            if (stateStoreFiles.file(file.getFilename()).isPresent()) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        try {
            validateFiles(stateStoreFiles);
        } catch (StateStoreException ex) {
            LOGGER.debug("Found invalid ingest commit for job {}", jobId, ex);
            return;
        }
        for (AllReferencesToAFile file : files) {
            stateStoreFiles.add(StateStoreFile.newFile(updateTime, file));
        }
    }

    @Override
    public boolean isEmpty() {
        return files.isEmpty();
    }

    /**
     * Reports the result of the transaction to the job tracker. This should be used after the transaction is fully
     * committed to the log.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     */
    public void reportJobCommit(IngestJobTracker tracker, TableStatus sleeperTable, StateStoreFiles stateBefore) {
        if (jobId == null) {
            return;
        }
        try {
            validateFiles(stateBefore);
            tracker.jobAddedFiles(createAddedEvent(sleeperTable));
        } catch (StateStoreException e) {
            tracker.jobFailed(createFailedEvent(sleeperTable, e));
        }
    }

    /**
     * Reports the result of the transaction to the job tracker. This should be used after the transaction is fully
     * committed to the log.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     */
    public void reportJobCommitOrThrow(IngestJobTracker tracker, TableStatus sleeperTable, StateStoreFiles stateBefore) {
        validateFiles(stateBefore);
        if (jobId == null) {
            return;
        }
        tracker.jobAddedFiles(createAddedEvent(sleeperTable));
    }

    /**
     * Reports failure of this transaction to the job tracker.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     * @param e            the failure
     */
    public void reportJobFailed(IngestJobTracker tracker, TableStatus sleeperTable, Exception e) {
        if (jobId == null) {
            return;
        }
        tracker.jobFailed(createFailedEvent(sleeperTable, e));
    }

    private IngestJobAddedFilesEvent createAddedEvent(TableStatus sleeperTable) {
        return IngestJobAddedFilesEvent.builder()
                .jobId(jobId).taskId(taskId).jobRunId(jobRunId)
                .tableId(sleeperTable.getTableUniqueId()).writtenTime(writtenTime)
                .files(files).build();
    }

    private IngestJobFailedEvent createFailedEvent(TableStatus sleeperTable, Exception e) {
        return IngestJobFailedEvent.builder()
                .jobId(jobId).taskId(taskId).jobRunId(jobRunId)
                .tableId(sleeperTable.getTableUniqueId())
                .failureTime(writtenTime)
                .failure(e)
                .build();
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
        return Objects.equals(jobId, other.jobId) && Objects.equals(taskId, other.taskId)
                && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(writtenTime, other.writtenTime)
                && Objects.equals(files, other.files);
    }

    @Override
    public String toString() {
        return "AddFilesTransaction{jobId=" + jobId + ", taskId=" + taskId + ", jobRunId=" + jobRunId + ", writtenTime="
                + writtenTime + ", files=" + files + "}";
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
         * Sets the IDs relating to this run of an ingest job. Used to update the job tracker based on the transaction
         * log. These should only be set if we need a job tracker update to happen against the transaction. Usually
         * for a synchronous commit the tracker will be updated separately.
         *
         * @param  jobRunIds the IDs
         * @return           the builder
         */
        public Builder jobRunIds(IngestJobRunIds jobRunIds) {
            return jobId(jobRunIds.getJobId())
                    .taskId(jobRunIds.getTaskId())
                    .jobRunId(jobRunIds.getJobRunId());
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

        /**
         * Sets the files to add to the state store.
         *
         * @param  fileReferences the file references to add
         * @return                this builder
         */
        public Builder fileReferences(List<FileReference> fileReferences) {
            return files(AllReferencesToAFile.newFilesWithReferences(fileReferences));
        }

        public AddFilesTransaction build() {
            return new AddFilesTransaction(this);
        }
    }
}
