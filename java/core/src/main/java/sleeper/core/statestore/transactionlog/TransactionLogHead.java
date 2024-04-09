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
import sleeper.core.util.ExponentialBackoffWithJitter;

import java.time.Instant;

class TransactionLogHead<T> {

    private final TransactionLogStore logStore;
    private final int maxAddTransactionAttempts;
    private final ExponentialBackoffWithJitter retryBackoff;
    private final Class<? extends StateStoreTransaction<T>> transactionType;
    private final T state;
    private long lastTransactionNumber = 0;

    private TransactionLogHead(
            TransactionLogStore logStore, int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff,
            Class<? extends StateStoreTransaction<T>> transactionType, T state) {
        this.logStore = logStore;
        this.maxAddTransactionAttempts = maxAddTransactionAttempts;
        this.retryBackoff = retryBackoff;
        this.transactionType = transactionType;
        this.state = state;
    }

    static TransactionLogHead<StateStoreFiles> forFiles(TransactionLogStore logStore, int retries, ExponentialBackoffWithJitter retryBackoff) {
        return new TransactionLogHead<StateStoreFiles>(logStore, retries, retryBackoff,
                FileReferenceTransaction.class, new StateStoreFiles());
    }

    static TransactionLogHead<StateStorePartitions> forPartitions(TransactionLogStore logStore, int retries, ExponentialBackoffWithJitter retryBackoff) {
        return new TransactionLogHead<StateStorePartitions>(logStore, retries, retryBackoff,
                PartitionTransaction.class, new StateStorePartitions());
    }

    void addTransaction(Instant updateTime, StateStoreTransaction<T> transaction) throws StateStoreException {
        Exception failure = new IllegalArgumentException("No attempts made");
        for (int attempt = 0; attempt < maxAddTransactionAttempts; attempt++) {
            try {
                retryBackoff.waitBeforeAttempt(attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StateStoreException("Interrupted while waiting to retry", e);
            }
            update();
            transaction.validate(state);
            long transactionNumber = lastTransactionNumber + 1;
            try {
                logStore.addTransaction(new TransactionLogEntry(transactionNumber, updateTime, transaction));
            } catch (Exception e) {
                failure = e;
                continue;
            }
            transaction.apply(state);
            lastTransactionNumber = transactionNumber;
            failure = null;
            break;
        }
        if (failure != null) {
            throw new StateStoreException("Failed adding transaction", failure);
        }
    }

    void update() throws StateStoreException {
        try {
            logStore.readTransactionsAfter(lastTransactionNumber)
                    .forEach(this::applyTransaction);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed reading transactions", e);
        }
    }

    private void applyTransaction(TransactionLogEntry entry) {
        if (!transactionType.isInstance(entry.getTransaction())) {
            return;
        }
        transactionType.cast(entry.getTransaction())
                .apply(state);
        lastTransactionNumber = entry.getTransactionNumber();
    }

    T state() {
        return state;
    }
}
