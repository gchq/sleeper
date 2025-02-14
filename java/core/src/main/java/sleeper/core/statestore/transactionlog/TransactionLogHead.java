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
import sleeper.core.statestore.transactionlog.log.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotLoader;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
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
public class TransactionLogHead<T> {
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

    public static Builder<?> builder() {
        return new Builder<>();
    }

    void addTransaction(Instant updateTime, StateStoreTransaction<T> transaction) throws StateStoreException {
        addTransaction(updateTime, AddTransactionRequest.withTransaction(transaction).build());
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
            prepareAddTransactionAttempt(attempt, request);
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

    private void prepareAddTransactionAttempt(int attempt, AddTransactionRequest request) throws StateStoreException {
        if (updateLogBeforeAddTransaction) {
            waitBeforeAttempt(attempt);
            forceUpdate();
            validate(request);
        } else if (attempt > 1) {
            waitBeforeAttempt(attempt - 1);
            forceUpdate();
            validate(request);
        } else {
            try {
                validate(request);
            } catch (StateStoreException e) {
                LOGGER.warn("Failed validating transaction on first attempt for table {}, will update from log and revalidate: {}", sleeperTable, e.getMessage());
                forceUpdate();
                validate(request);
            }
        }
    }

    private void validate(AddTransactionRequest request) throws StateStoreException {
        if (request.isCommitIfInvalid()) {
            return;
        }
        Instant startTime = Instant.now();
        StateStoreTransaction<T> transaction = request.getTransaction();
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
        TransactionLogEntry entry = TransactionLogEntry.fromRequest(transactionNumber, updateTime, request);
        try {
            logStore.addTransaction(entry);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed adding transaction", e);
        }
        StateStoreTransaction<T> transaction = request.getTransaction();
        request.getBeforeApplyListener().beforeApply(entry, transaction, state);
        Instant startApplyTime = Instant.now();
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
    public void update() throws StateStoreException {
        Instant startTime = stateUpdateClock.get();
        if (nextTransactionCheckTime != null && startTime.isBefore(nextTransactionCheckTime)) {
            LOGGER.debug("Not checking for {} transactions for table {}, next check at {}",
                    state.getClass().getSimpleName(), sleeperTable, nextTransactionCheckTime);
            return;
        }
        update(startTime, TransactionLogRange.toUpdateLocalStateAt(lastTransactionNumber));
    }

    private void forceUpdate() throws StateStoreException {
        forceUpdate(TransactionLogRange.toUpdateLocalStateAt(lastTransactionNumber));
    }

    private void forceUpdate(TransactionLogRange range) throws StateStoreException {
        update(stateUpdateClock.get(), range);
    }

    private void update(Instant startTime, TransactionLogRange range) {
        try {
            Instant afterSnapshotTime = loadSnapshotIfNeeded(startTime, range);
            updateFromLog(afterSnapshotTime, range);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed updating state from transactions", e);
        }
    }

    private Instant loadSnapshotIfNeeded(Instant startTime, TransactionLogRange range) {
        if (nextSnapshotCheckTime != null && startTime.isBefore(nextSnapshotCheckTime)) {
            LOGGER.debug("Not checking for snapshot of {} for table {}, next check at {}",
                    state.getClass().getSimpleName(), sleeperTable, nextSnapshotCheckTime);
            return startTime;
        }
        long minTransactionNumberToLoadSnapshot = minTransactionNumberToLoadSnapshot();
        Optional<TransactionLogSnapshot> snapshotOpt = range.withMinTransactionNumber(minTransactionNumberToLoadSnapshot)
                .flatMap(snapshotRange -> snapshotLoader.loadLatestSnapshotInRange(snapshotRange));
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

    private void updateFromLog(Instant startTime, TransactionLogRange range) {
        long transactionNumberBeforeLogLoad = lastTransactionNumber;
        LOGGER.debug("Updating {} for table {} from log from transaction {}",
                state.getClass().getSimpleName(), sleeperTable, lastTransactionNumber);
        range.withMinTransactionNumber(transactionNumberBeforeLogLoad + 1)
                .ifPresent(readRange -> logStore.readTransactions(readRange).forEach(this::applyTransaction));
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

    void applyTransactionUpdatingIfNecessary(TransactionLogEntry entry, StateListenerBeforeApply listener) {
        long entryNumber = entry.getTransactionNumber();
        if (entryNumber <= lastTransactionNumber) {
            throw new StateStoreException("Attempted to apply transaction out of order. " +
                    "Last transaction applied was " + lastTransactionNumber + ", found transaction " + entryNumber + ".");
        }
        if (lastTransactionNumber < (entryNumber - 1)) { // If we're not up to date with the given transaction
            forceUpdate(TransactionLogRange.toUpdateLocalStateToApply(lastTransactionNumber, entryNumber));
        }
        applyTransaction(entry, listener);
    }

    private void applyTransaction(TransactionLogEntry entry) {
        applyTransaction(entry, StateListenerBeforeApply.none());
    }

    private void applyTransaction(TransactionLogEntry entry, StateListenerBeforeApply listener) {
        if (!transactionType.isAssignableFrom(entry.getTransactionType().getType())) {
            LOGGER.warn("Found unexpected transaction type for table {} with number {}. Expected {}, found {}",
                    sleeperTable, entry.getTransactionNumber(),
                    transactionType.getName(),
                    entry.getTransactionType());
            return;
        }
        StateStoreTransaction<T> transaction = transactionType.cast(
                entry.getTransactionOrLoadFromPointer(sleeperTable.getTableUniqueId(), transactionBodyStore));
        listener.beforeApply(entry, transaction, state);
        transaction.apply(state, entry.getUpdateTime());
        lastTransactionNumber = entry.getTransactionNumber();
    }

    T state() {
        return state;
    }

    public long getLastTransactionNumber() {
        return lastTransactionNumber;
    }

    /**
     * Builder to initialise the head to read from a transaction log and snapshots.
     *
     * @param <T> the type of the state derived from the log
     */
    public static class Builder<T> {
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

        /**
         * Sets the Sleeper table whose state this interacts with.
         *
         * @param  sleeperTable the Sleeper table status
         * @return              this builder
         */
        public Builder<T> sleeperTable(TableStatus sleeperTable) {
            this.sleeperTable = sleeperTable;
            return this;
        }

        /**
         * Sets the store that holds the underlying transaction log.
         *
         * @param  logStore the store
         * @return          this builder
         */
        public Builder<T> logStore(TransactionLogStore logStore) {
            this.logStore = logStore;
            return this;
        }

        /**
         * Sets the store that holds large transactions that do not fit directly in the log.
         *
         * @param  transactionBodyStore the store
         * @return                      this builder
         */
        public Builder<T> transactionBodyStore(TransactionBodyStore transactionBodyStore) {
            this.transactionBodyStore = transactionBodyStore;
            return this;
        }

        /**
         * Sets whether to update from the log before the first attempt to add a transaction. Adding a transaction will
         * fail if the local state is out of date with the transaction log. In that case, further attempts will be made
         * after bringing the local state up to date from the transaction log. Defaults to always update the local state
         * first.
         *
         * @param  updateLogBeforeAddTransaction true if we should update the local state before the first attempt to
         *                                       add a new transaction
         * @return                               this builder
         */
        public Builder<T> updateLogBeforeAddTransaction(boolean updateLogBeforeAddTransaction) {
            this.updateLogBeforeAddTransaction = updateLogBeforeAddTransaction;
            return this;
        }

        /**
         * Sets the number of times to attempt to add a new transaction to the log. If another process is adding
         * transactions to the same log at the same time, only one will be successful for a given transaction number.
         * This is the number of times a process may fail due to a conflict before it stops retrying.
         *
         * @param  maxAddTransactionAttempts the number of attempts to add a new transaction
         * @return                           this builder
         */
        public Builder<T> maxAddTransactionAttempts(int maxAddTransactionAttempts) {
            this.maxAddTransactionAttempts = maxAddTransactionAttempts;
            return this;
        }

        /**
         * Sets the behaviour for how long to wait during retries to add a new transaction to the log.
         *
         * @param  retryBackoff the settings for retry backoff with jitter
         * @return              this builder
         */
        public Builder<T> retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        /**
         * Sets the type of transaction that may be used with this log. This should be either a partition transaction or
         * a file transaction.
         *
         * @param  <N>             the type of the state that transactions will operate on
         * @param  transactionType the class of the generic transaction type
         * @return                 this builder
         * @see                    #forFiles()
         * @see                    #forPartitions()
         */
        public <N> Builder<N> transactionType(Class<? extends StateStoreTransaction<N>> transactionType) {
            this.transactionType = (Class<? extends StateStoreTransaction<T>>) transactionType;
            return (Builder<N>) this;
        }

        /**
         * Sets the loader to retrieve the latest snapshot of the state.
         *
         * @param  snapshotLoader the loader
         * @return                this builder
         */
        public Builder<T> snapshotLoader(TransactionLogSnapshotLoader snapshotLoader) {
            this.snapshotLoader = snapshotLoader;
            return this;
        }

        /**
         * Sets the amount of time to wait in between checks for a new snapshot. Defaults to zero.
         *
         * @param  timeBetweenSnapshotChecks the amount of time to wait
         * @return                           this builder
         */
        public Builder<T> timeBetweenSnapshotChecks(Duration timeBetweenSnapshotChecks) {
            this.timeBetweenSnapshotChecks = timeBetweenSnapshotChecks;
            return this;
        }

        /**
         * Sets the amount of time to wait in between checks for new transactions. Defaults to zero.
         *
         * @param  timeBetweenTransactionChecks the amount of time to wait
         * @return                              this builder
         */
        public Builder<T> timeBetweenTransactionChecks(Duration timeBetweenTransactionChecks) {
            this.timeBetweenTransactionChecks = timeBetweenTransactionChecks;
            return this;
        }

        /**
         * Sets the clock to provide the time of a new transaction. Defaults to Instant.now().
         *
         * @param  stateUpdateClock the clock
         * @return                  this builder
         */
        public Builder<T> stateUpdateClock(Supplier<Instant> stateUpdateClock) {
            this.stateUpdateClock = stateUpdateClock;
            return this;
        }

        /**
         * Sets the state object to keep up to date with the transaction log. This will be mutated as transactions are
         * read from or added to the log. This must match the type required by transactions in the log. Note that if a
         * snapshot is loaded later, this state object will be discarded and replaced by the loaded snapshot.
         *
         * @param  state the state object
         * @return       this builder
         * @see          #forFiles()
         * @see          #forPartitions()
         */
        public Builder<T> state(T state) {
            this.state = state;
            return this;
        }

        /**
         * Sets the last transaction number that was read from the log. Defaults to zero, meaning no transactions have
         * been read. Usually the only case this will not be zero is if the state object was loaded from a snapshot.
         *
         * @param  lastTransactionNumber the last transaction number read from the log
         * @return                       this builder
         */
        public Builder<T> lastTransactionNumber(long lastTransactionNumber) {
            this.lastTransactionNumber = lastTransactionNumber;
            return this;
        }

        /**
         * The minimum number of transactions that the local state should be behind a snapshot in order to load the
         * state from the snapshot instead of from the transaction log. This is to avoid loading a large snapshot when
         * it is more efficient to get up to date from the log.
         *
         * @param  minTransactionsAheadToLoadSnapshot the minimum number of transactions ahead to load a snapshot
         * @return                                    this builder
         */
        public Builder<T> minTransactionsAheadToLoadSnapshot(long minTransactionsAheadToLoadSnapshot) {
            this.minTransactionsAheadToLoadSnapshot = minTransactionsAheadToLoadSnapshot;
            return this;
        }

        /**
         * Sets the transaction type and initial state object to work with file transactions.
         *
         * @return this builder
         */
        public Builder<StateStoreFiles> forFiles() {
            return transactionType(FileReferenceTransaction.class)
                    .state(new StateStoreFiles());
        }

        /**
         * Sets the transaction type and initial state object to work with partition transactions.
         *
         * @return this builder
         */
        public Builder<StateStorePartitions> forPartitions() {
            return transactionType(PartitionTransaction.class)
                    .state(new StateStorePartitions());
        }

        public TransactionLogHead<T> build() {
            return new TransactionLogHead<>(this);
        }
    }
}
