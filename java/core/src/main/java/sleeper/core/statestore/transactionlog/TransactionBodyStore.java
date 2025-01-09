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

/**
 * A store of the bodies of transactions that will be referenced in a transaction log. Used by
 * {@link TransactionLogStateStore} for transactions that are too large to be held directly in
 * {@link TransactionLogStore}.
 */
public interface TransactionBodyStore {

    /**
     * Stores a transaction at a given location.
     *
     * @param pointer     a pointer to the location
     * @param transaction the transaction
     */
    void store(TransactionBodyPointer pointer, StateStoreTransaction<?> transaction);

    /**
     * Retrives a transaction from a given location.
     *
     * @param  pointer a pointer to the location
     * @return         the transaction
     */
    <T extends StateStoreTransaction<?>> T getBody(TransactionBodyPointer pointer);
}
