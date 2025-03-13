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
package sleeper.core.statestore;

import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFailedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A request to apply the results of a compaction in the state store. The old references will be deleted and a new
 * reference will be created in one transaction.
 */
public class ReplaceFileReferencesRequest {
    private final String jobId;
    private final String taskId;
    private final String jobRunId;
    private final List<String> inputFiles;
    private final FileReference newReference;

    /**
     * Creates a request to commit one job.
     *
     * @param  jobId        the job ID
     * @param  inputFiles   the filenames of the job's input files
     * @param  newReference the new reference to replace the input file references on the partition
     *
     * @return              the request
     */
    public static ReplaceFileReferencesRequest replaceJobFileReferences(
            String jobId, List<String> inputFiles, FileReference newReference) {
        return ReplaceFileReferencesRequest.builder()
                .jobId(jobId)
                .inputFiles(inputFiles)
                .newReference(newReference)
                .build();
    }

    private ReplaceFileReferencesRequest(Builder builder) {
        jobId = Objects.requireNonNull(builder.jobId, "jobId must not be null");
        taskId = builder.taskId;
        jobRunId = builder.jobRunId;
        inputFiles = Objects.requireNonNull(builder.inputFiles, "inputFiles must not be null");
        newReference = Objects.requireNonNull(builder.newReference, "newReference must not be null");
        if (taskId != null) {
            Objects.requireNonNull(jobRunId, "jobRunId must not be null when taskId is set");
        }
        if (jobRunId != null) {
            Objects.requireNonNull(taskId, "taskId must not be null when jobRunId is set");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a copy of this request with the update time removed in the file reference. Used when storing a
     * transaction to apply this request where the update time is held separately.
     *
     * @return the copy
     */
    public ReplaceFileReferencesRequest withNoUpdateTime() {
        return builder().jobId(jobId).taskId(taskId).jobRunId(jobRunId).inputFiles(inputFiles)
                .newReference(newReference.toBuilder().lastStateStoreUpdateTime(null).build())
                .build();
    }

    /**
     * Validates the request against the current state.
     *
     * @param  stateStoreFiles                        the state
     * @throws FileNotFoundException                  if an input file does not exist
     * @throws FileReferenceNotFoundException         if an input file is not referenced on the same partition
     * @throws FileReferenceNotAssignedToJobException if an input file is not assigned to the job on this partition
     * @throws FileAlreadyExistsException             if the new file already exists in the state store
     */
    public void validateStateChange(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (String filename : inputFiles) {
            StateStoreFile file = stateStoreFiles.file(filename)
                    .orElseThrow(() -> new FileNotFoundException(filename));
            FileReference reference = file.getReferenceForPartitionId(newReference.getPartitionId())
                    .orElseThrow(() -> new FileReferenceNotFoundException(filename, newReference.getPartitionId()));
            if (!jobId.equals(reference.getJobId())) {
                throw new FileReferenceNotAssignedToJobException(reference, jobId);
            }
        }
        if (stateStoreFiles.file(newReference.getFilename()).isPresent()) {
            throw new FileAlreadyExistsException(newReference.getFilename());
        }
    }

    /**
     * Validates that the output file is not the same as any of the input files.
     *
     * @throws NewReferenceSameAsOldReferenceException thrown if any of the input files are the same as the output file
     */
    public void validateNewReference() throws NewReferenceSameAsOldReferenceException {
        for (String inputFile : inputFiles) {
            if (inputFile.equals(newReference.getFilename())) {
                throw new NewReferenceSameAsOldReferenceException(inputFile);
            }
        }
    }

    /**
     * Reports a commit or failure based on validity of the job in the state before the transaction. This should be
     * used after the transaction is fully committed to the log.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     * @param stateBefore  the state before the transaction was applied
     * @param now          the current time
     */
    public void reportCommitted(CompactionJobTracker tracker, TableStatus sleeperTable, StateStoreFiles stateBefore, Instant now) {
        if (taskId == null) {
            return;
        }
        try {
            validateStateChange(stateBefore);
            tracker.jobCommitted(createCommittedEvent(sleeperTable, now));
        } catch (StateStoreException e) {
            tracker.jobFailed(createFailedEvent(sleeperTable, now, e));
        }
    }

    /**
     * Reports that the job failed to commit.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     * @param now          the current time
     * @param e            the failure
     */
    public void reportFailed(CompactionJobTracker tracker, TableStatus sleeperTable, Instant now, Exception e) {
        if (taskId == null) {
            return;
        }
        tracker.jobFailed(createFailedEvent(sleeperTable, now, e));
    }

    private CompactionJobCommittedEvent createCommittedEvent(TableStatus sleeperTable, Instant now) {
        return CompactionJobCommittedEvent.builder()
                .jobId(jobId)
                .tableId(sleeperTable.getTableUniqueId())
                .taskId(taskId)
                .jobRunId(jobRunId)
                .commitTime(now)
                .build();
    }

    private CompactionJobFailedEvent createFailedEvent(TableStatus sleeperTable, Instant now, Exception e) {
        return CompactionJobFailedEvent.builder()
                .jobId(jobId)
                .tableId(sleeperTable.getTableUniqueId())
                .taskId(taskId)
                .jobRunId(jobRunId)
                .failureTime(now)
                .failure(e)
                .build();
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public String getPartitionId() {
        return newReference.getPartitionId();
    }

    public List<String> getInputFiles() {
        return inputFiles;
    }

    public FileReference getNewReference() {
        return newReference;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, taskId, jobRunId, inputFiles, newReference);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReplaceFileReferencesRequest)) {
            return false;
        }
        ReplaceFileReferencesRequest other = (ReplaceFileReferencesRequest) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(taskId, other.taskId) && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(inputFiles, other.inputFiles)
                && Objects.equals(newReference, other.newReference);
    }

    @Override
    public String toString() {
        return "ReplaceFileReferencesRequest{jobId=" + jobId + ", taskId=" + taskId + ", jobRunId=" + jobRunId + ", inputFiles=" + inputFiles + ", newReference=" + newReference + "}";
    }

    /**
     * Builder for replace file references requests.
     */
    public static final class Builder {

        private String jobId;
        private String taskId;
        private String jobRunId;
        private List<String> inputFiles;
        private FileReference newReference;

        private Builder() {
        }

        public ReplaceFileReferencesRequest build() {
            return new ReplaceFileReferencesRequest(this);
        }

        /**
         * Sets the ID of the compaction job being committed.
         *
         * @param  jobId the job ID
         * @return       this builder
         */
        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        /**
         * Sets the ID of the compaction task that ran the job. If set this is used by the compaction job tracker.
         *
         * @param  taskId the task ID
         * @return        this builder
         */
        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        /**
         * Sets the ID of the job run. If set this is used by the compaction job tracker.
         *
         * @param  jobRunId the job run ID
         * @return          this builder
         */
        public Builder jobRunId(String jobRunId) {
            this.jobRunId = jobRunId;
            return this;
        }

        /**
         * Sets the filenames of the job's input files.
         *
         * @param  inputFiles the input files
         * @return            this builder
         */
        public Builder inputFiles(List<String> inputFiles) {
            this.inputFiles = inputFiles;
            return this;
        }

        /**
         * Sets the new reference to replace the input file references on the partition.
         *
         * @param  newReference the output file reference
         * @return              this builder
         */
        public Builder newReference(FileReference newReference) {
            this.newReference = newReference;
            return this;
        }

    }
}
