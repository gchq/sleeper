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

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A transaction to remove a number of file references that were assigned to a job, and replace them with a new file.
 * This can be used to apply the results of a compaction.
 */
public class ReplaceFileReferencesTransaction implements FileReferenceTransaction {

    private final String jobId;
    private final String partitionId;
    private final List<String> inputFiles;
    private final FileReference newReference;

    public ReplaceFileReferencesTransaction(
            String jobId, String partitionId, List<String> inputFiles, FileReference newReference)
            throws StateStoreException {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.inputFiles = inputFiles;
        this.newReference = newReference.toBuilder().lastStateStoreUpdateTime(null).build();
        FileReference.validateNewReferenceForJobOutput(inputFiles, newReference);
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (String filename : inputFiles) {
            AllReferencesToAFile file = stateStoreFiles.file(filename)
                    .orElseThrow(() -> new FileNotFoundException(filename));
            FileReference reference = file.getReferenceForPartitionId(partitionId)
                    .orElseThrow(() -> new FileReferenceNotFoundException(filename, partitionId));
            if (!jobId.equals(reference.getJobId())) {
                throw new FileReferenceNotAssignedToJobException(reference, jobId);
            }
        }
        if (stateStoreFiles.file(newReference.getFilename()).isPresent()) {
            throw new FileAlreadyExistsException(newReference.getFilename());
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (String filename : inputFiles) {
            stateStoreFiles.updateFile(filename, file -> file.removeReferenceForPartition(partitionId, updateTime));
        }
        stateStoreFiles.add(AllReferencesToAFile.fileWithOneReference(newReference, updateTime));
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, partitionId, inputFiles, newReference);
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
        return Objects.equals(jobId, other.jobId) && Objects.equals(partitionId, other.partitionId) && Objects.equals(inputFiles, other.inputFiles) && Objects.equals(newReference, other.newReference);
    }

    @Override
    public String toString() {
        return "ReplaceFileReferencesTransaction{jobId=" + jobId + ", partitionId=" + partitionId + ", inputFiles=" + inputFiles + ", newReference=" + newReference + "}";
    }
}
