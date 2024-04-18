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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.LoggedDuration;

import java.time.Instant;

class TransactionLogHead<T> {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogHead.class);

    private final TableStatus sleeperTable;
    private final TransactionLogStore logStore;
    private final int maxAddTransactionAttempts;
    private final ExponentialBackoffWithJitter retryBackoff;
    private final Class<? extends StateStoreTransaction<T>> transactionType;
    private final T state;
    private long lastTransactionNumber = 0;

    TransactionLogHead(
            TableStatus sleeperTable, TransactionLogStore logStore,
            int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff,
            Class<? extends StateStoreTransaction<T>> transactionType, T state) {
        this.sleeperTable = sleeperTable;
        this.logStore = logStore;
        this.maxAddTransactionAttempts = maxAddTransactionAttempts;
        this.retryBackoff = retryBackoff;
        this.transactionType = transactionType;
        this.state = state;
    }

    static TransactionLogHead<StateStoreFiles> forFiles(
            TableStatus sleeperTable, TransactionLogStore logStore,
            int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff) {
        return new TransactionLogHead<StateStoreFiles>(
                sleeperTable, logStore, maxAddTransactionAttempts, retryBackoff,
                FileReferenceTransaction.class, new StateStoreFiles());
    }

    static TransactionLogHead<StateStorePartitions> forPartitions(
            TableStatus sleeperTable, TransactionLogStore logStore,
            int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff) {
        return new TransactionLogHead<StateStorePartitions>(
                sleeperTable, logStore, maxAddTransactionAttempts, retryBackoff,
                PartitionTransaction.class, new StateStorePartitions());
    }

    void addTransaction(Instant updateTime, StateStoreTransaction<T> transaction) throws StateStoreException {
        Instant startTime = Instant.now();
        LOGGER.info("Adding transaction of type {} to table {}",
                transaction.getClass().getSimpleName(), sleeperTable);
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
                LOGGER.warn("Failed adding transaction after {} retries for table {}, failure: {}",
                        attempt, sleeperTable, e);
                failure = e;
                continue;
            }
            transaction.apply(state, updateTime);
            lastTransactionNumber = transactionNumber;
            failure = null;
            LOGGER.info("Added transaction of type {} to table {} with {} retries, took {}",
                    transaction.getClass().getSimpleName(), sleeperTable, attempt,
                    LoggedDuration.withShortOutput(startTime, Instant.now()));
            break;
        }
        if (failure != null) {
            throw new StateStoreException("Failed adding transaction", failure);
        }
    }

    void update() throws StateStoreException {
        try {
            Instant startTime = Instant.now();
            long transactionNumberBefore = lastTransactionNumber;
            logStore.readTransactionsAfter(lastTransactionNumber)
                    .forEach(this::applyTransaction);
            LOGGER.info("Updated {}, read {} transactions, took {}, last transaction number is {}",
                    state.getClass().getSimpleName(), lastTransactionNumber - transactionNumberBefore,
                    LoggedDuration.withShortOutput(startTime, Instant.now()), lastTransactionNumber);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed reading transactions", e);
        }
    }

    private void applyTransaction(TransactionLogEntry entry) {
        if (!transactionType.isInstance(entry.getTransaction())) {
            LOGGER.warn("Found unexpected transaction type. Expected {}, found {}",
                    transactionType.getClass().getName(), entry.getTransaction().getClass().getName());
            return;
        }
        transactionType.cast(entry.getTransaction())
                .apply(state, entry.getUpdateTime());
        lastTransactionNumber = entry.getTransactionNumber();
    }

    T state() {
        return state;
    }

    long lastTransactionNumber() {
        return lastTransactionNumber;
    }
}
