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

import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A transaction to assign files to jobs.
 */
public class AssignJobIdsTransaction implements FileReferenceTransaction {

    private final List<AssignJobIdRequest> requests;

    public AssignJobIdsTransaction(List<AssignJobIdRequest> requests) {
        this.requests = requests;
    }

    /**
     * Creates a transaction to apply a list of job assignment requests. Discards any requests that do not assign any
     * files.
     *
     * @param  requests the list of requests to apply
     * @return          the transaction, if any requests assign any files to a job
     */
    public static Optional<AssignJobIdsTransaction> ignoringEmptyRequests(List<AssignJobIdRequest> requests) {
        List<AssignJobIdRequest> filtered = requests.stream()
                .filter(request -> !request.getFilenames().isEmpty())
                .collect(toUnmodifiableList());
        if (filtered.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new AssignJobIdsTransaction(filtered));
        }
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (AssignJobIdRequest request : requests) {
            for (String filename : request.getFilenames()) {
                StateStoreFile existingFile = stateStoreFiles.file(filename)
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
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (AssignJobIdRequest request : requests) {
            for (String filename : request.getFilenames()) {
                stateStoreFiles.updateFile(filename,
                        file -> file.setJobIdForPartition(request.getJobId(), request.getPartitionId(), updateTime));
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(requests);
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
        return Objects.equals(requests, other.requests);
    }

    @Override
    public String toString() {
        return "AssignJobIdsTransaction{requests=" + requests + "}";
    }

}
