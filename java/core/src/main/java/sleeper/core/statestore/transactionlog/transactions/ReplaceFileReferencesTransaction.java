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
package sleeper.core.statestore.transactionlog.transactions;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A transaction to remove a number of file references that were assigned to a job, and replace them with a new file.
 * This can be used to apply the results of a compaction.
 */
public class ReplaceFileReferencesTransaction implements FileReferenceTransaction {

    private final List<ReplaceFileReferencesRequest> jobs;

    public ReplaceFileReferencesTransaction(List<ReplaceFileReferencesRequest> jobs) throws StateStoreException {
        this.jobs = jobs.stream()
                .map(job -> job.withNoUpdateTime())
                .collect(toUnmodifiableList());
        for (ReplaceFileReferencesRequest job : jobs) {
            FileReference.validateNewReferenceForJobOutput(job.getInputFiles(), job.getNewReference());
        }
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        // Compactions are committed in big batches. We ensure file references are assigned to a job before it is run,
        // so the impact of any invalid commits should be limited.
        // Instead of failing completely if a commit is invalid, we discard any invalid jobs at the point when we apply
        // the transaction in the apply method.
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (ReplaceFileReferencesRequest job : jobs) {
            try {
                validateJob(stateStoreFiles, job);
            } catch (StateStoreException e) {
                continue;
            }
            for (String filename : job.getInputFiles()) {
                stateStoreFiles.updateFile(filename, file -> file.removeReferenceForPartition(job.getPartitionId(), updateTime));
            }
            stateStoreFiles.add(StateStoreFile.newFile(updateTime, job.getNewReference()));
        }
    }

    private void validateJob(StateStoreFiles stateStoreFiles, ReplaceFileReferencesRequest job) {
        for (String filename : job.getInputFiles()) {
            StateStoreFile file = stateStoreFiles.file(filename)
                    .orElseThrow(() -> new FileNotFoundException(filename));
            FileReference reference = file.getReferenceForPartitionId(job.getPartitionId())
                    .orElseThrow(() -> new FileReferenceNotFoundException(filename, job.getPartitionId()));
            if (!job.getJobId().equals(reference.getJobId())) {
                throw new FileReferenceNotAssignedToJobException(reference, job.getJobId());
            }
        }
        if (stateStoreFiles.file(job.getNewReference().getFilename()).isPresent()) {
            throw new FileAlreadyExistsException(job.getNewReference().getFilename());
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
