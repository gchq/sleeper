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
     * Applies a transaction to the state store.
     *
     * @param request the request
     * @see           AddTransactionRequest
     */
    default void addTransaction(AddTransactionRequest request) {
        if (request.getTransaction() instanceof FileReferenceTransaction) {
            addFilesTransaction(request);
        } else if (request.getTransaction() instanceof PartitionTransaction) {
            addPartitionsTransaction(request);
        }
    }
}
