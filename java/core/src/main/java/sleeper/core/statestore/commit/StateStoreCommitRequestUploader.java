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
package sleeper.core.statestore.commit;

import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStoreProvider;
import sleeper.core.statestore.transactionlog.TransactionBodyStoreProviderByTableId;

/**
 * Handles uploading transactions to S3 if a commit request is too big to fit in an SQS message.
 */
public class StateStoreCommitRequestUploader {
    public static final int MAX_SQS_LENGTH = 262144;

    private final TransactionBodyStoreProviderByTableId transactionBodyStore;
    private final StateStoreCommitRequestSerDe serDe;
    private final int maxLength;

    public StateStoreCommitRequestUploader(
            TablePropertiesProvider tablePropertiesProvider, TransactionBodyStoreProvider transactionBodyStore, int maxLength) {
        this(transactionBodyStore.byTableId(tablePropertiesProvider), new StateStoreCommitRequestSerDe(tablePropertiesProvider), maxLength);
    }

    public StateStoreCommitRequestUploader(
            TransactionBodyStoreProviderByTableId transactionBodyStore, StateStoreCommitRequestSerDe serDe, int maxLength) {
        this.transactionBodyStore = transactionBodyStore;
        this.serDe = serDe;
        this.maxLength = maxLength;
    }

    /**
     * Serialises a commit request to a string, uploading the transaction to S3 first if it is too big.
     *
     * @param  request the request
     * @return         the string
     */
    public String serialiseAndUploadIfTooBig(StateStoreCommitRequest request) {
        String json = serDe.toJson(request);
        if (json.length() < maxLength) {
            return json;
        } else {
            String key = TransactionBodyStore.createObjectKey(request.getTableId());
            transactionBodyStore.getTransactionBodyStore(request.getTableId()).store(key, request.getTableId(), request.getTransactionIfHeld().orElseThrow());
            return serDe.toJson(StateStoreCommitRequest.create(request.getTableId(), key, request.getTransactionType()));
        }
    }
}
