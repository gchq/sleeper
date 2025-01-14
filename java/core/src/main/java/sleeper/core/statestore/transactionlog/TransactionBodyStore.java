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
package sleeper.core.statestore.transactionlog;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;

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
     * @param transaction the transaction
     */
    void store(String key, StateStoreTransaction<?> transaction);

    /**
     * Retrives a transaction from a given location.
     *
     * @param  key the object key in the data bucket
     * @return     the transaction
     */
    <T extends StateStoreTransaction<?>> T getBody(String key, TransactionType transactionType);

    /**
     * Creates an object key for a new transaction file with a randomly generated filename. The file will not yet exist.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the object key
     */
    static String createObjectKey(TableProperties tableProperties) {
        // Use a random UUID to avoid conflicting when another process is adding a transaction at the same time
        return tableProperties.get(TABLE_ID) + "/statestore/transactions/" + Instant.now() + "-" + UUID.randomUUID().toString() + ".json";
    }
}
