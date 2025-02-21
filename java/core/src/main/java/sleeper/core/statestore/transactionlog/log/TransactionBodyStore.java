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
package sleeper.core.statestore.transactionlog.log;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.time.Instant;
import java.util.UUID;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * A store of the bodies of transactions that will be referenced in a transaction log. Used by
 * {@link TransactionLogStateStore} for transactions that are too large to be held directly in
 * {@link TransactionLogStore}.
 */
public interface TransactionBodyStore {

    /**
     * Stores a transaction at a given location.
     *
     * @param key         the object key in the data bucket
     * @param tableId     the ID of the Sleeper table the transaction is for
     * @param transaction the transaction
     */
    void store(String key, String tableId, StateStoreTransaction<?> transaction);

    /**
     * Stores a transaction if it is too large to fit in the associated log store.
     *
     * @param  tableId the ID of the Sleeper table the transaction is for
     * @param  request the request to add the transaction
     * @return         the new request, identical to the original but with a pointer to the new transaction entry if it
     *                 was uploaded
     */
    default AddTransactionRequest storeIfTooBig(String tableId, AddTransactionRequest request) {
        return request;
    }

    /**
     * Retrives a transaction from a given location.
     *
     * @param  key             the object key in the data bucket
     * @param  tableId         the ID of the Sleeper table the transaction is for
     * @param  transactionType the type of the transaction
     * @return                 the transaction
     */
    <T extends StateStoreTransaction<?>> T getBody(String key, String tableId, TransactionType transactionType);

    /**
     * Retrives a transaction from commit request.
     *
     * @param  request the commit request
     * @return         the transaction
     */
    default <T extends StateStoreTransaction<?>> T getTransaction(StateStoreCommitRequest request) {
        return request.<T>getTransactionIfHeld()
                .orElseGet(() -> getBody(request.getBodyKey(), request.getTableId(), request.getTransactionType()));
    }

    /**
     * Creates an object key for a new transaction file with a randomly generated filename. The file will not yet exist.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the object key
     */
    static String createObjectKey(TableProperties tableProperties) {
        return createObjectKey(tableProperties.get(TABLE_ID));
    }

    /**
     * Creates an object key for a new transaction file with a randomly generated filename. The file will not yet exist.
     *
     * @param  tableId the Sleeper table ID
     * @return         the object key
     */
    static String createObjectKey(String tableId) {
        // Use a random UUID to avoid conflicting when another process is adding a transaction at the same time
        return createObjectKey(tableId, Instant.now(), UUID.randomUUID().toString());
    }

    /**
     * Creates an object key for a new transaction file with a randomly generated filename. The file will not yet exist.
     *
     * @param  tableId the Sleeper table ID
     * @param  now     the time now
     * @param  uuid    a random UUID
     * @return         the object key
     */
    static String createObjectKey(String tableId, Instant now, String uuid) {
        return tableId + "/statestore/transactions/" + now + "-" + uuid + ".json";
    }
}
