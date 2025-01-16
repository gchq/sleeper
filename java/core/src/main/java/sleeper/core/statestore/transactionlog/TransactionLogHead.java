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

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Tracks some state derived from a transaction log, at a position in the log. This can perform an update to the state
 * by adding a transaction to the log, or it can bring the state up to date with transactions in the log.
 * <p>
 * Interacts with the log with {@link TransactionLogStore}. Can skip reading transactions when a snapshot is available
 * of the state at a specific point in the log with {@link TransactionLogSnapshotLoader}.
 *
 * @param <T> the type of the state derived from the log
 */
class TransactionLogHead<T> {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogHead.class);

    private final TableStatus sleeperTable;
    private final TransactionLogStore logStore;
    private final TransactionBodyStore transactionBodyStore;
    private final boolean updateLogBeforeAddTransaction;
    private final int maxAddTransactionAttempts;
    private final ExponentialBackoffWithJitter retryBackoff;
    private final Class<? extends StateStoreTransaction<T>> transactionType;
    private final TransactionLogSnapshotLoader snapshotLoader;
    private final Duration timeBetweenSnapshotChecks;
    private final Duration timeBetweenTransactionChecks;
    private final Supplier<Instant> stateUpdateClock;
    private final long minTransactionsAheadToLoadSnapshot;
    private T state;
    private long lastTransactionNumber;
    private Instant nextSnapshotCheckTime;
    private Instant nextTransactionCheckTime;

    private TransactionLogHead(Builder<T> builder) {
        sleeperTable = builder.sleeperTable;
        logStore = builder.logStore;
        transactionBodyStore = builder.transactionBodyStore;
        updateLogBeforeAddTransaction = builder.updateLogBeforeAddTransaction;
        maxAddTransactionAttempts = builder.maxAddTransactionAttempts;
        retryBackoff = builder.retryBackoff;
        transactionType = builder.transactionType;
        snapshotLoader = builder.snapshotLoader;
        timeBetweenSnapshotChecks = builder.timeBetweenSnapshotChecks;
        timeBetweenTransactionChecks = builder.timeBetweenTransactionChecks;
        stateUpdateClock = builder.stateUpdateClock;
        minTransactionsAheadToLoadSnapshot = builder.minTransactionsAheadToLoadSnapshot;
        state = builder.state;
        lastTransactionNumber = builder.lastTransactionNumber;
    }

    static Builder<?> builder() {
        return new Builder<>();
    }

    void addTransaction(Instant updateTime, StateStoreTransaction<T> transaction) throws StateStoreException {
        addTransaction(updateTime, AddTransactionRequest.transaction(transaction));
    }

    /**
     * Adds a transaction to the log. Brings the state as up to date as possible before writing to the log. Handles
     * conflicts with other processes writing to the log at the same time.
     *
     * @throws StateStoreException thrown if there's any failure reading or adding a transaction
     */
    void addTransaction(Instant updateTime, AddTransactionRequest request) throws StateStoreException {
        Instant startTime = Instant.now();
        LOGGER.debug("Adding transaction of type {} to table {}",
                request.getTransactionType(), sleeperTable);
        Exception failure = new IllegalArgumentException("No attempts made");
        for (int attempt = 1; attempt <= maxAddTransactionAttempts; attempt++) {
            prepareAddTransactionAttempt(attempt, request.getTransaction());
            try {
                attemptAddTransaction(updateTime, request);
                LOGGER.info("Added transaction of type {} to table {} with {} attempts, took {}",
                        request.getTransactionType(), sleeperTable, attempt,
                        LoggedDuration.withShortOutput(startTime, Instant.now()));
                return;
            } catch (DuplicateTransactionNumberException e) {
                LOGGER.warn("Failed adding transaction on attempt {} of {} for table {}, failure: {}",
                        attempt, maxAddTransactionAttempts, sleeperTable, e.toString());
                failure = e;
            }
        }
        LOGGER.error("Failed adding transaction with {} attempts, took {}",
                maxAddTransactionAttempts, LoggedDuration.withShortOutput(startTime, Instant.now()));
        throw new StateStoreException("Failed adding transaction", failure);
    }

    private void prepareAddTransactionAttempt(int attempt, StateStoreTransaction<T> transaction) throws StateStoreException {
        if (updateLogBeforeAddTransaction) {
            waitBeforeAttempt(attempt);
            forceUpdate();
            validate(transaction);
        } else if (attempt > 1) {
            waitBeforeAttempt(attempt - 1);
            forceUpdate();
            validate(transaction);
        } else {
            try {
                validate(transaction);
            } catch (StateStoreException e) {
                LOGGER.warn("Failed validating transaction on first attempt for table {}, will update from log and revalidate: {}", sleeperTable, e.getMessage());
                forceUpdate();
                validate(transaction);
            }
        }
    }

    private void validate(StateStoreTransaction<T> transaction) throws StateStoreException {
        Instant startTime = Instant.now();
        transaction.validate(state);
        LOGGER.debug("Validated transaction in {}", LoggedDuration.withShortOutput(startTime, Instant.now()));
    }

    private void waitBeforeAttempt(int attempt) throws StateStoreException {
        try {
            retryBackoff.waitBeforeAttempt(attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StateStoreException("Interrupted while waiting to retry", e);
        }
    }

    private void attemptAddTransaction(Instant updateTime, AddTransactionRequest request) throws StateStoreException, DuplicateTransactionNumberException {
        long transactionNumber = lastTransactionNumber + 1;
        try {
            logStore.addTransaction(TransactionLogEntry.fromRequest(transactionNumber, updateTime, request));
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed adding transaction", e);
        }
        request.getBeforeApplyListener().beforeApply(transactionNumber, state);
        Instant startApplyTime = Instant.now();
        StateStoreTransaction<T> transaction = request.getTransaction();
        transaction.apply(state, updateTime);
        lastTransactionNumber = transactionNumber;
        LOGGER.debug("Applied transaction {} in {}",
                transactionNumber, LoggedDuration.withShortOutput(startApplyTime, Instant.now()));
    }

    /**
     * Brings the state up to date with the log by seeking through transactions starting at the current position in the
     * log. Applies each transaction to the state in order.
     *
     * @throws StateStoreException thrown if there's any failure reading transactions or applying them to the state
     */
    void update() throws StateStoreException {
        try {
            Instant startTime = stateUpdateClock.get();
            if (nextTransactionCheckTime != null && startTime.isBefore(nextTransactionCheckTime)) {
                LOGGER.debug("Not checking for {} transactions for table {}, next check at {}",
                        state.getClass().getSimpleName(), sleeperTable, nextTransactionCheckTime);
                return;
            }
            Instant afterSnapshotTime = loadSnapshotIfNeeded(startTime);
            updateFromLog(afterSnapshotTime);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed updating state from transactions", e);
        }
    }

    private void forceUpdate() throws StateStoreException {
        try {
            Instant startTime = stateUpdateClock.get();
            Instant afterSnapshotTime = loadSnapshotIfNeeded(startTime);
            updateFromLog(afterSnapshotTime);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed force updating state from transactions", e);
        }
    }

    private Instant loadSnapshotIfNeeded(Instant startTime) {
        if (nextSnapshotCheckTime != null && startTime.isBefore(nextSnapshotCheckTime)) {
            LOGGER.debug("Not checking for snapshot of {} for table {}, next check at {}",
                    state.getClass().getSimpleName(), sleeperTable, nextSnapshotCheckTime);
            return startTime;
        }
        long minTransactionNumberToLoadSnapshot = minTransactionNumberToLoadSnapshot();
        Optional<TransactionLogSnapshot> snapshotOpt = snapshotLoader
                .loadLatestSnapshotIfAtMinimumTransaction(minTransactionNumberToLoadSnapshot);
        Instant finishTime = stateUpdateClock.get();
        nextSnapshotCheckTime = finishTime.plus(timeBetweenSnapshotChecks);
        snapshotOpt.ifPresentOrElse(snapshot -> {
            state = snapshot.getState();
            lastTransactionNumber = snapshot.getTransactionNumber();
            LOGGER.info("Loaded snapshot of {} for table {} at transaction {} in {}",
                    state.getClass().getSimpleName(), sleeperTable, lastTransactionNumber,
                    LoggedDuration.withShortOutput(startTime, finishTime));
        }, () -> {
            LOGGER.debug("No snapshot found of {} for table {} at or beyond transaction {} in {}",
                    state.getClass().getSimpleName(), sleeperTable, minTransactionNumberToLoadSnapshot,
                    LoggedDuration.withShortOutput(startTime, finishTime));
        });
        return finishTime;
    }

    private long minTransactionNumberToLoadSnapshot() {
        if (lastTransactionNumber < 1) { // Always load snapshot when no transactions have been read yet
            return 1;
        } else {
            return lastTransactionNumber + minTransactionsAheadToLoadSnapshot;
        }
    }

    private void updateFromLog(Instant startTime) {
        long transactionNumberBeforeLogLoad = lastTransactionNumber;
        LOGGER.debug("Updating {} for table {} from log from transaction {}",
                state.getClass().getSimpleName(), sleeperTable, lastTransactionNumber);
        logStore.readTransactionsAfter(lastTransactionNumber)
                .forEach(this::applyTransaction);
        long readTransactions = lastTransactionNumber - transactionNumberBeforeLogLoad;
        Instant finishTime = stateUpdateClock.get();
        nextTransactionCheckTime = finishTime.plus(timeBetweenTransactionChecks);
        if (readTransactions > 0) {
            LOGGER.info("Updated {} for table {}, read {} transactions from log in {}, last transaction number is {}",
                    state.getClass().getSimpleName(), sleeperTable,
                    readTransactions,
                    LoggedDuration.withShortOutput(startTime, finishTime),
                    lastTransactionNumber);
        } else {
            LOGGER.debug("No new transactions found in log of {} for table {} in {}, last transaction number is {}",
                    state.getClass().getSimpleName(), sleeperTable,
                    LoggedDuration.withShortOutput(startTime, finishTime),
                    lastTransactionNumber);
        }
    }

    private void applyTransaction(TransactionLogEntry entry) {
        if (!transactionType.isAssignableFrom(entry.getTransactionType().getType())) {
            LOGGER.warn("Found unexpected transaction type for table {} with number {}. Expected {}, found {}",
                    sleeperTable, entry.getTransactionNumber(),
                    transactionType.getName(),
                    entry.getTransactionType());
            return;
        }
        transactionType.cast(entry.getTransactionOrLoadFromPointer(transactionBodyStore))
                .apply(state, entry.getUpdateTime());
        lastTransactionNumber = entry.getTransactionNumber();
    }

    T state() {
        return state;
    }

    long lastTransactionNumber() {
        return lastTransactionNumber;
    }

    /**
     * Builder to initialise the head to read from a transaction log and snapshots.
     *
     * @param <T> the type of the state derived from the log
     */
    static class Builder<T> {
        private TableStatus sleeperTable;
        private TransactionLogStore logStore;
        private TransactionBodyStore transactionBodyStore;
        private boolean updateLogBeforeAddTransaction = true;
        private int maxAddTransactionAttempts;
        private ExponentialBackoffWithJitter retryBackoff;
        private Class<? extends StateStoreTransaction<T>> transactionType;
        private TransactionLogSnapshotLoader snapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private Duration timeBetweenSnapshotChecks = Duration.ZERO;
        private Duration timeBetweenTransactionChecks = Duration.ZERO;
        private Supplier<Instant> stateUpdateClock = Instant::now;
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

        public Builder<T> transactionBodyStore(TransactionBodyStore transactionBodyStore) {
            this.transactionBodyStore = transactionBodyStore;
            return this;
        }

        public Builder<T> updateLogBeforeAddTransaction(boolean updateLogBeforeAddTransaction) {
            this.updateLogBeforeAddTransaction = updateLogBeforeAddTransaction;
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

        public Builder<T> timeBetweenSnapshotChecks(Duration timeBetweenSnapshotChecks) {
            this.timeBetweenSnapshotChecks = timeBetweenSnapshotChecks;
            return this;
        }

        public Builder<T> timeBetweenTransactionChecks(Duration timeBetweenTransactionChecks) {
            this.timeBetweenTransactionChecks = timeBetweenTransactionChecks;
            return this;
        }

        public Builder<T> stateUpdateClock(Supplier<Instant> stateUpdateClock) {
            this.stateUpdateClock = stateUpdateClock;
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
            return transactionType(FileReferenceTransaction.class)
                    .state(new StateStoreFiles());
        }

        public Builder<StateStorePartitions> forPartitions() {
            return transactionType(PartitionTransaction.class)
                    .state(new StateStorePartitions());
        }

        public TransactionLogHead<T> build() {
            return new TransactionLogHead<>(this);
        }
    }
}
