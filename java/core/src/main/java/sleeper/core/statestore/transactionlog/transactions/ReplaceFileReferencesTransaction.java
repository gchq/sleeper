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
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogHead;

import java.time.Instant;
import java.util.List;

public class ReplaceFileReferencesTransaction implements StateStoreTransaction {

    private final String jobId;
    private final String partitionId;
    private final List<String> inputFiles;
    private final FileReference newReference;
    private final Instant updateTime;

    public ReplaceFileReferencesTransaction(
            String jobId, String partitionId, List<String> inputFiles, FileReference newReference, Instant updateTime)
            throws StateStoreException {
        this.jobId = jobId;
        this.partitionId = partitionId;
        this.inputFiles = inputFiles;
        this.newReference = newReference;
        this.updateTime = updateTime;
        FileReference.validateNewReferenceForJobOutput(inputFiles, newReference);
    }

    @Override
    public void validate(TransactionLogHead state) throws StateStoreException {
        for (String filename : inputFiles) {
            AllReferencesToAFile file = state.files().file(filename)
                    .orElseThrow(() -> new FileNotFoundException(filename));
            FileReference reference = file.getReferenceForPartitionId(partitionId)
                    .orElseThrow(() -> new FileReferenceNotFoundException(filename, partitionId));
            if (!jobId.equals(reference.getJobId())) {
                throw new FileReferenceNotAssignedToJobException(reference, jobId);
            }
        }
        if (state.files().file(newReference.getFilename()).isPresent()) {
            throw new FileAlreadyExistsException(newReference.getFilename());
        }
    }

    @Override
    public void apply(TransactionLogHead state) {
        state.files().replaceFiles(partitionId, inputFiles, newReference, updateTime);
    }

    // For linting, since this field is only used to keep a record
    String getJobId() {
        return jobId;
    }
}
