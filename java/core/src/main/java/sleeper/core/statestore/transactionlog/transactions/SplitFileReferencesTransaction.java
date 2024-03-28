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
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.transactionlog.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;

public class SplitFileReferencesTransaction implements FileReferenceTransaction {

    private final List<SplitFileReferenceRequest> requests;
    private final Instant updateTime;

    public SplitFileReferencesTransaction(List<SplitFileReferenceRequest> requests, Instant updateTime) {
        this.requests = requests.stream()
                .map(SplitFileReferenceRequest::withNoUpdateTimes)
                .collect(toUnmodifiableList());
        this.updateTime = updateTime;
    }

    @Override
    public void validate(StateStoreFiles stateStoreFiles) throws StateStoreException {
        for (SplitFileReferenceRequest request : requests) {
            AllReferencesToAFile file = stateStoreFiles.file(request.getFilename())
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
    public void apply(StateStoreFiles stateStoreFiles) {
        for (SplitFileReferenceRequest request : requests) {
            stateStoreFiles.updateFile(request.getFilename(),
                    file -> file.splitReferenceFromPartition(request.getFromPartitionId(), request.getNewReferences(), updateTime));
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
        if (!(obj instanceof SplitFileReferencesTransaction)) {
            return false;
        }
        SplitFileReferencesTransaction other = (SplitFileReferencesTransaction) obj;
        return Objects.equals(requests, other.requests) && Objects.equals(updateTime, other.updateTime);
    }

    @Override
    public String toString() {
        return "SplitFileReferencesTransaction{requests=" + requests + ", updateTime=" + updateTime + "}";
    }
}
