/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.statestorev2.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;

/**
 * Uploads transactions to S3 if they are larger than a certain limit.
 */
public class StateStoreCommitRequestUploader {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitRequestUploader.class);

    private final TransactionBodyStore transactionBodyStore;
    private final StateStoreCommitRequestSerDe requestSerDe;

    public StateStoreCommitRequestUploader(
            TransactionBodyStore transactionBodyStore, TransactionSerDeProvider transactionSerDeProvider) {
        this.transactionBodyStore = transactionBodyStore;
        this.requestSerDe = new StateStoreCommitRequestSerDe(transactionSerDeProvider);
    }

    /**
     * Serialises a commit request to a string, uploading the transaction to S3 if it is too big.
     *
     * @param  request the request
     * @return         the string
     */
    public String serialiseAndUploadIfTooBig(StateStoreCommitRequest request) {
        return requestSerDe.toJson(request.getTransactionIfHeld()
                .map(transaction -> uploadTransactionIfTooBig(request, transaction))
                .orElse(request));
    }

    private StateStoreCommitRequest uploadTransactionIfTooBig(StateStoreCommitRequest request, StateStoreTransaction<?> transaction) {
        return transactionBodyStore.storeIfTooBig(request.getTableId(), transaction)
                .getBodyKey()
                .map(key -> StateStoreCommitRequest.create(request.getTableId(), key, request.getTransactionType()))
                .orElse(request);
    }

}
