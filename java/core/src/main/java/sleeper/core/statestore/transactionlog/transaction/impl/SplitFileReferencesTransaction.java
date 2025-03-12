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

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateStoreFile;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Performs atomic updates to split file references. This is used to push file references down the partition tree,
 * eg. where records are ingested to a non-leaf partition, or when a partition is split. A file referenced in a
 * larger, non-leaf partition may be split between smaller partitions which cover non-overlapping sub-ranges of the
 * original partition. This includes these records in compactions of the descendent partitions.
 * <p>
 * The aim is to combine all records into a small number of files for each leaf partition, where the leaves of the
 * partition tree should represent a separation of the data into manageable chunks. Compaction operates on file
 * references to pull records from multiple files into one, when they are referenced in the same partition. This
 * reduces the number of files in the system, and improves statistics and indexing within each partition. This
 * should result in faster queries, and more accurate partitioning when a partition is split.
 * <p>
 * Each {@link SplitFileReferenceRequest} will remove one file reference, and create new references to the same file
 * in descendent partitions. The reference counts will be tracked accordingly.
 * <p>
 * The ranges covered by the partitions of the new references must not overlap, so there must never be two references to
 * the same file where one partition is a descendent of the other.
 * <p>
 * Currently this is always done atomically, but future implementations of the state store may not fit this in a single
 * transaction. Each {@link SplitFileReferenceRequest} is guaranteed to be done atomically in one transaction, but it is
 * possible that some may succeed and some may fail. If a single {@link SplitFileReferenceRequest} adds too many
 * references to apply in one transaction, this will also fail.
 */
public class SplitFileReferencesTransaction implements FileReferenceTransaction {

    private final List<SplitFileReferenceRequest> requests;

    public SplitFileReferencesTransaction(List<SplitFileReferenceRequest> requests) {
        this.requests = requests.stream()
                .map(SplitFileReferenceRequest::withNoUpdateTimes)
                .collect(toUnmodifiableList());
    }

    /**
     * Commit this transaction directly to the state store without going to the commit queue. This will throw any
     * validation exceptions immediately, even if they wouldn't be as part of an asynchronous commit.
     *
     * @param  stateStore                   the state store
     * @throws SplitRequestsFailedException if any of the requests fail, even if some succeeded
     */
    public void synchronousCommit(StateStore stateStore) {
        try {
            stateStore.addFilesTransaction(AddTransactionRequest.withTransaction(this).build());
        } catch (StateStoreException e) {
            throw new SplitRequestsFailedException(List.of(), requests, e);
        }
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (SplitFileReferenceRequest request : requests) {
            StateStoreFile file = stateStoreFiles.file(request.getFilename())
                    .orElseThrow(() -> new FileNotFoundException(request.getFilename()));
            FileReference oldReference = file.getReferenceForPartitionId(request.getFromPartitionId())
                    .orElseThrow(() -> new FileReferenceNotFoundException(request.getOldReference()));
            if (oldReference.getJobId() != null) {
                throw new FileReferenceAssignedToJobException(oldReference);
            }
            for (FileReference newReference : request.getNewReferences()) {
                if (file.getReferenceForPartitionId(newReference.getPartitionId()).isPresent()) {
                    throw new FileReferenceAlreadyExistsException(newReference);
                }
            }
        }
    }

    @Override
    public void apply(StateStoreFiles stateStoreFiles, Instant updateTime) {
        for (SplitFileReferenceRequest request : requests) {
            stateStoreFiles.updateFile(request.getFilename(),
                    file -> file.splitReferenceFromPartition(request.getFromPartitionId(), request.getNewReferences(), updateTime));
        }
    }

    @Override
    public boolean isEmpty() {
        return requests.isEmpty();
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
        if (!(obj instanceof SplitFileReferencesTransaction)) {
            return false;
        }
        SplitFileReferencesTransaction other = (SplitFileReferencesTransaction) obj;
        return Objects.equals(requests, other.requests);
    }

    @Override
    public String toString() {
        return "SplitFileReferencesTransaction{requests=" + requests + "}";
    }
}
