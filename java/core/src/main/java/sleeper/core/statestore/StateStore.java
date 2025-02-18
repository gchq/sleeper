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
package sleeper.core.statestore;

import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;

/**
 * Stores information about the data files and their status (i.e. {@link FileReference}s,
 * and the {@link sleeper.core.partition.Partition}s).
 */
public interface StateStore extends FileReferenceStore, PartitionStore {
    /**
     * Clears all file data and partition data from the state store. Note that this does not delete any of the actual
     * files, and after calling this method the store must be initialised before the Sleeper table can be used again.
     *
     * @throws StateStoreException if the update fails
     */
    default void clearSleeperTable() throws StateStoreException {
        clearFileData();
        clearPartitionData();
    }

    /**
     * Adds a transaction to the transaction log. The transaction may or may not already be held in S3. If it is already
     * held in S3, we don't need to write it to S3 again.
     *
     * @param request the request
     */
    default void addTransaction(AddTransactionRequest request) {
        if (request.getTransaction() instanceof FileReferenceTransaction) {
            addFilesTransaction(request);
        } else if (request.getTransaction() instanceof PartitionTransaction) {
            addPartitionsTransaction(request);
        }
    }
}
