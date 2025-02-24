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
package sleeper.core.statestore.testutils;

import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An in memory store of the bodies of transactions that are not held directly in the log.
 */
public class InMemoryTransactionBodyStore implements TransactionBodyStore {

    private final Map<String, StateStoreTransaction<?>> transactionByKey = new HashMap<>();
    private boolean storeTransactions = false;
    private Function<String, String> createObjectKey = TransactionBodyStore::createObjectKey;

    @Override
    public void store(String key, String tableId, StateStoreTransaction<?> transaction) {
        transactionByKey.put(key, transaction);
    }

    @Override
    public AddTransactionRequest storeIfTooBig(String tableId, AddTransactionRequest request) {
        if (!storeTransactions) {
            return request;
        }
        String key = createObjectKey.apply(tableId);
        transactionByKey.put(key, request.getTransaction());
        return request.toBuilder().bodyKey(key).build();
    }

    @Override
    public <T extends StateStoreTransaction<?>> T getBody(String key, String tableId, TransactionType transactionType) {
        T transaction = (T) Objects.requireNonNull(transactionByKey.get(key));
        Class<?> expectedClass = transactionType.getType();
        if (!expectedClass.isInstance(transaction)) {
            throw new IllegalArgumentException("Expected stored transaction to be of type " + expectedClass.getName() + ", found " + transaction.getClass().getName());
        }
        return transaction;
    }

    public void setStoreTransactions(boolean storeTransactions) {
        this.storeTransactions = storeTransactions;
    }

    /**
     * Turns on storing transactions in the store, and supplies locations where they should be held. After this is
     * called, every transaction added to the log will have its body held in this store, with one of the given object
     * keys. The object keys will be used in order until none are left, at which point adding a transaction will fail.
     *
     * @param keys the object keys representing where each transaction is held
     */
    public void setStoreTransactionsWithObjectKeys(List<String> keys) {
        setStoreTransactions(true);
        setCreateObjectKey(keys.iterator()::next);
    }

    private void setCreateObjectKey(Supplier<String> keySupplier) {
        this.createObjectKey = tableId -> keySupplier.get();
    }
}
