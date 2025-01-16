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

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStoreProvider;

/**
 * Handles uploading transactions to S3 if a commit request is too big to fit in an SQS message.
 */
public class StateStoreCommitRequestUploader {
    private final TablePropertiesProvider tablePropertiesProvider;
    private final TransactionBodyStoreProvider transactionBodyStore;
    private final StateStoreCommitRequestByTransactionSerDe serDe;
    private final int maxBodyLength;

    public StateStoreCommitRequestUploader(
            TablePropertiesProvider tablePropertiesProvider, TransactionBodyStoreProvider transactionBodyStore, int maxBodyLength) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.transactionBodyStore = transactionBodyStore;
        this.maxBodyLength = maxBodyLength;
        this.serDe = new StateStoreCommitRequestByTransactionSerDe(tablePropertiesProvider);
    }

    /**
     * Serialises a commit request to a string, uploading the transaction to S3 first if it is too big.
     *
     * @param  request the request
     * @return         the string
     */
    public String serialiseAndUploadIfTooBig(StateStoreCommitRequestByTransaction request) {
        String json = serDe.toJson(request);
        if (json.length() < maxBodyLength) {
            return json;
        } else {
            TableProperties tableProperties = tablePropertiesProvider.getById(request.getTableId());
            String key = TransactionBodyStore.createObjectKey(tableProperties);
            transactionBodyStore.getTransactionBodyStore(tableProperties).store(key, request.getTransactionIfHeld().orElseThrow());
            return serDe.toJson(StateStoreCommitRequestByTransaction.create(request.getTableId(), key, request.getTransactionType()));
        }
    }
}
