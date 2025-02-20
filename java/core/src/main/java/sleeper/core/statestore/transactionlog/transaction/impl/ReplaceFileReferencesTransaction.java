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

import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A transaction to remove a number of file references that were assigned to a job, and replace them with a new file.
 * This can be used to apply the results of a compaction.
 */
public class ReplaceFileReferencesTransaction implements FileReferenceTransaction {
    public static final Logger LOGGER = LoggerFactory.getLogger(ReplaceFileReferencesTransaction.class);

    private final List<ReplaceFileReferencesRequest> jobs;

    public ReplaceFileReferencesTransaction(List<ReplaceFileReferencesRequest> jobs) throws StateStoreException {
        this.jobs = jobs.stream()
                .map(job -> job.withNoUpdateTime())
                .collect(toUnmodifiableList());
        for (ReplaceFileReferencesRequest job : jobs) {
            job.validateNewReference();
        }
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        // Compactions are committed in big batches, so we want to avoid the whole batch failing.
        // We ensure file references are assigned to a job before it is run, which should prevent the files getting into
        // an invalid or unexpected state.
        // Instead of failing completely if a commit is invalid, we discard any invalid jobs at the point when we apply
        // the transaction in the apply method.
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (ReplaceFileReferencesRequest job : jobs) {
            try {
                job.validateStateChange(stateStoreFiles);
            } catch (StateStoreException e) {
                LOGGER.debug("Found invalid compaction commit for job {}", job.getJobId(), e);
                continue;
            }
            for (String filename : job.getInputFiles()) {
                stateStoreFiles.updateFile(filename, file -> file.removeReferenceForPartition(job.getPartitionId(), updateTime));
            }
            stateStoreFiles.add(StateStoreFile.newFile(updateTime, job.getNewReference()));
        }
    }

    /**
     * Reports commits and failures based on validity of each job in the state before the transaction. This should be
     * used after the transaction is fully committed to the log.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     * @param stateBefore  the state before the transaction was applied
     * @param now          the current time
     */
    public void reportJobCommits(CompactionJobTracker tracker, TableStatus sleeperTable, StateStoreFiles stateBefore, Instant now) {
        for (ReplaceFileReferencesRequest job : jobs) {
            if (job.getTaskId() == null) {
                continue;
            }
            try {
                job.validateStateChange(stateBefore);
                tracker.jobCommitted(job.createCommittedEvent(sleeperTable, now));
            } catch (StateStoreException e) {
                tracker.jobFailed(job.createFailedEvent(sleeperTable, now, e));
            }
        }
    }

    /**
     * Reports that the whole transaction failed to commit.
     *
     * @param tracker      the job tracker
     * @param sleeperTable the table being updated
     * @param now          the current time
     */
    public void reportJobsAllFailed(CompactionJobTracker tracker, TableStatus sleeperTable, Instant now, Exception e) {
        for (ReplaceFileReferencesRequest job : jobs) {
            if (job.getTaskId() == null) {
                continue;
            }
            tracker.jobFailed(job.createFailedEvent(sleeperTable, now, e));
        }
    }

    /**
     * Validates the transaction against the current state. Used during a synchronous commit to report on any
     * failures.
     *
     * @param  stateStoreFiles                         the state
     * @throws FileNotFoundException                   if an input file does not exist
     * @throws FileReferenceNotFoundException          if an input file is not referenced on the same partition
     * @throws FileReferenceNotAssignedToJobException  if an input file is not assigned to the job on this partition
     * @throws FileAlreadyExistsException              if the new file already exists in the state store
     * @throws NewReferenceSameAsOldReferenceException if any of the input files are the same as the output file
     */
    public void validateStateChange(StateStoreFiles stateStoreFiles) {
        for (ReplaceFileReferencesRequest job : jobs) {
            job.validateNewReference();
            job.validateStateChange(stateStoreFiles);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ReplaceFileReferencesTransaction)) {
            return false;
        }
        ReplaceFileReferencesTransaction other = (ReplaceFileReferencesTransaction) obj;
        return Objects.equals(jobs, other.jobs);
    }

    @Override
    public String toString() {
        return "ReplaceFileReferencesTransaction{jobs=" + jobs + "}";
    }
}
