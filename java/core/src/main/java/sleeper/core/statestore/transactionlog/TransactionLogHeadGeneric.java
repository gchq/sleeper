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

import sleeper.core.statestore.StateStoreException;

public class TransactionLogHeadGeneric<T> {

    private final TransactionLogStore logStore;
    private final Class<StateStoreTransactionGeneric<T>> transactionType;
    private final T state;
    private long lastTransactionNumber = 0;

    public TransactionLogHeadGeneric(
            TransactionLogStore logStore, Class<StateStoreTransactionGeneric<T>> transactionType, T state) {
        this.logStore = logStore;
        this.transactionType = transactionType;
        this.state = state;
    }

    public void addTransaction(StateStoreTransactionGeneric<T> transaction) throws StateStoreException {
        update();
        transaction.validate(state);
        long transactionNumber = lastTransactionNumber + 1;
        logStore.addTransaction(transaction, transactionNumber);
        transaction.apply(state);
        lastTransactionNumber = transactionNumber;
    }

    public void update() {
        logStore.readTransactionsAfter(lastTransactionNumber)
                .filter(transactionType::isInstance)
                .map(transactionType::cast)
                .forEach(transaction -> {
                    transaction.apply(state);
                    lastTransactionNumber++;
                });
    }

    public T state() {
        return state;
    }
}
