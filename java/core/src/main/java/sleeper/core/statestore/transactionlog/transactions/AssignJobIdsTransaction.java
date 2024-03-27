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
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogHead;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class AssignJobIdsTransaction implements StateStoreTransaction {

    private final List<AssignJobIdRequest> requests;
    private final Instant updateTime;

    public AssignJobIdsTransaction(List<AssignJobIdRequest> requests, Instant updateTime) {
        this.requests = requests;
        this.updateTime = updateTime;
    }

    @Override
    public void validate(TransactionLogHead state) throws StateStoreException {
        for (AssignJobIdRequest request : requests) {
            for (String filename : request.getFilenames()) {
                AllReferencesToAFile existingFile = state.files().file(filename)
                        .orElseThrow(() -> new FileReferenceNotFoundException(filename, request.getPartitionId()));
                FileReference existingReference = existingFile.getReferenceForPartitionId(request.getPartitionId())
                        .orElseThrow(() -> new FileReferenceNotFoundException(filename, request.getPartitionId()));
                if (existingReference.getJobId() != null) {
                    throw new FileReferenceAssignedToJobException(existingReference);
                }
            }
        }
    }

    @Override
    public void apply(TransactionLogHead state) {
        for (AssignJobIdRequest request : requests) {
            for (String filename : request.getFilenames()) {
                state.files().updateFile(filename,
                        file -> file.withJobIdForPartition(request.getJobId(), request.getPartitionId(), updateTime));
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(requests, updateTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AssignJobIdsTransaction)) {
            return false;
        }
        AssignJobIdsTransaction other = (AssignJobIdsTransaction) obj;
        return Objects.equals(requests, other.requests) && Objects.equals(updateTime, other.updateTime);
    }

    @Override
    public String toString() {
        return "AssignJobIdsTransaction{requests=" + requests + ", updateTime=" + updateTime + "}";
    }

}
