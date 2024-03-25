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
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogHead;

import java.time.Instant;
import java.util.List;

public class SplitFileReferencesTransaction implements StateStoreTransaction {

    private final List<SplitFileReferenceRequest> requests;
    private final Instant updateTime;

    public SplitFileReferencesTransaction(List<SplitFileReferenceRequest> requests, Instant updateTime) {
        this.requests = requests;
        this.updateTime = updateTime;
    }

    @Override
    public void validate(TransactionLogHead state) throws StateStoreException {
        for (SplitFileReferenceRequest request : requests) {
            AllReferencesToAFile file = state.files().file(request.getFilename())
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
    public void apply(TransactionLogHead state) {
        for (SplitFileReferenceRequest request : requests) {
            state.files().updateFile(request.getFilename(),
                    file -> file.splitReferenceFromPartition(request.getFromPartitionId(), request.getNewReferences(), updateTime));
        }
    }

}
