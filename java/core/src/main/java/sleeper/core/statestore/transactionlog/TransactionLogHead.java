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
    private final TransactionLogSnapshotLoader snapshotLoader;
    private final long minTransactionsAheadToLoadSnapshot;
    private T state;
    private long lastTransactionNumber;

    private TransactionLogHead(Builder<T> builder) {
        this.sleeperTable = builder.sleeperTable;
        this.logStore = builder.logStore;
        this.maxAddTransactionAttempts = builder.maxAddTransactionAttempts;
        this.retryBackoff = builder.retryBackoff;
        this.transactionType = builder.transactionType;
        this.snapshotLoader = builder.snapshotLoader;
        this.minTransactionsAheadToLoadSnapshot = builder.minTransactionsAheadToLoadSnapshot;
        this.state = builder.state;
        this.lastTransactionNumber = builder.lastTransactionNumber;
    }

    static Builder<?> builder() {
        return new Builder<>();
    }

    void addTransaction(Instant updateTime, StateStoreTransaction<T> transaction) throws StateStoreException {
        Instant startTime = Instant.now();
        LOGGER.info("Adding transaction of type {} to table {}",
                transaction.getClass().getSimpleName(), sleeperTable);
        Exception failure = new IllegalArgumentException("No attempts made");
        for (int attempts = 0; attempts < maxAddTransactionAttempts; attempts++) {
            try {
                retryBackoff.waitBeforeAttempt(attempts);
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
                LOGGER.warn("Failed adding transaction on attempt {} of {} for table {}, failure: {}",
                        attempts + 1, maxAddTransactionAttempts, sleeperTable, e.toString());
                failure = e;
                continue;
            }
            transaction.apply(state, updateTime);
            lastTransactionNumber = transactionNumber;
            failure = null;
            LOGGER.info("Added transaction of type {} to table {} with {} attempts, took {}",
                    transaction.getClass().getSimpleName(), sleeperTable, attempts + 1,
                    LoggedDuration.withShortOutput(startTime, Instant.now()));
            break;
        }
        if (failure != null) {
            LOGGER.error("Failed adding transaction with {} attempts, took {}",
                    maxAddTransactionAttempts, LoggedDuration.withShortOutput(startTime, Instant.now()));
            throw new StateStoreException("Failed adding transaction", failure);
        }
    }

    void update() throws StateStoreException {
        try {
            Instant startTime = Instant.now();
            long transactionNumberBefore = lastTransactionNumber;
            snapshotLoader.loadLatestSnapshotIfAtMinimumTransaction(
                    transactionNumberBefore + minTransactionsAheadToLoadSnapshot)
                    .ifPresent(snapshot -> {
                        state = snapshot.getState();
                        lastTransactionNumber = snapshot.getTransactionNumber();
                    });
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

    static class Builder<T> {
        private TableStatus sleeperTable;
        private TransactionLogStore logStore;
        private int maxAddTransactionAttempts;
        private ExponentialBackoffWithJitter retryBackoff;
        private Class<? extends StateStoreTransaction<T>> transactionType;
        private TransactionLogSnapshotLoader snapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private T state;
        private long lastTransactionNumber = 0;
        private long minTransactionsAheadToLoadSnapshot = 1;

        private Builder() {
        }

        public Builder<T> sleeperTable(TableStatus sleeperTable) {
            this.sleeperTable = sleeperTable;
            return this;
        }

        public Builder<T> logStore(TransactionLogStore logStore) {
            this.logStore = logStore;
            return this;
        }

        public Builder<T> maxAddTransactionAttempts(int maxAddTransactionAttempts) {
            this.maxAddTransactionAttempts = maxAddTransactionAttempts;
            return this;
        }

        public Builder<T> retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public <N> Builder<N> transactionType(Class<? extends StateStoreTransaction<N>> transactionType) {
            this.transactionType = (Class<? extends StateStoreTransaction<T>>) transactionType;
            return (Builder<N>) this;
        }

        public Builder<T> snapshotLoader(TransactionLogSnapshotLoader snapshotLoader) {
            this.snapshotLoader = snapshotLoader;
            return this;
        }

        public Builder<T> state(T state) {
            this.state = state;
            return this;
        }

        public Builder<T> lastTransactionNumber(long lastTransactionNumber) {
            this.lastTransactionNumber = lastTransactionNumber;
            return this;
        }

        public Builder<T> minTransactionsAheadToLoadSnapshot(long minTransactionsAheadToLoadSnapshot) {
            this.minTransactionsAheadToLoadSnapshot = minTransactionsAheadToLoadSnapshot;
            return this;
        }

        public Builder<StateStoreFiles> forFiles() {
            return transactionType(FileReferenceTransaction.class);
        }

        public Builder<StateStorePartitions> forPartitions() {
            return transactionType(PartitionTransaction.class);
        }

        public TransactionLogHead<T> build() {
            return new TransactionLogHead<>(this);
        }
    }
}
