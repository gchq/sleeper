/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.statestore.DelegatingStateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotLoader;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.core.util.ThreadSleep;

import java.time.Duration;
import java.time.Instant;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;

/**
 * A state store implementation where state is derived from a transaction log. Dependent on an implementation of a
 * transaction log and snapshots.
 */
public class TransactionLogStateStore extends DelegatingStateStore {

    public static final int DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS = 10;
    public static final WaitRange DEFAULT_RETRY_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(0.2, 30);
    public static final long DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT = 10;
    public static final Duration DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS = Duration.ofMinutes(1);
    public static final Duration DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS = Duration.ZERO;

    private final TransactionLogFileReferenceStore files;
    private final TransactionLogPartitionStore partitions;

    private TransactionLogStateStore(Builder builder) {
        this(builder, TransactionLogHead.builder()
                .tableProperties(builder.tableProperties)
                .transactionBodyStore(builder.transactionBodyStore)
                .updateLogBeforeAddTransaction(builder.updateLogBeforeAddTransaction)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        TransactionLogHead.retryWaitRange(builder.tableProperties),
                        builder.randomJitterFraction,
                        builder.retryWaiter)));
    }

    private TransactionLogStateStore(Builder builder, TransactionLogHead.Builder<?> headBuilder) {
        this(headBuilder.forFiles()
                .logStore(builder.filesLogStore)
                .snapshotLoader(builder.filesSnapshotLoader)
                .stateUpdateClock(builder.filesStateUpdateClock)
                .build(),
                headBuilder.forPartitions()
                        .logStore(builder.partitionsLogStore)
                        .snapshotLoader(builder.partitionsSnapshotLoader)
                        .stateUpdateClock(builder.partitionsStateUpdateClock)
                        .build());
    }

    private TransactionLogStateStore(TransactionLogHead<StateStoreFiles> filesHead, TransactionLogHead<StateStorePartitions> partitionsHead) {
        this(new TransactionLogFileReferenceStore(filesHead),
                new TransactionLogPartitionStore(partitionsHead));
    }

    private TransactionLogStateStore(TransactionLogFileReferenceStore files, TransactionLogPartitionStore partitions) {
        super(files, partitions);
        this.files = files;
        this.partitions = partitions;
    }

    /**
     * Updates the local state from the transaction logs.
     *
     * @throws StateStoreException thrown if there's any failure reading transactions or applying them to the state
     */
    public void updateFromLogs() throws StateStoreException {
        files.updateFromLog();
        partitions.updateFromLog();
    }

    /**
     * Applies a transaction log entry to the local state, and applies some action based on the state before it. Will
     * read from the transaction log if entries are missing between the last read entry and the given entry. The given
     * entry must already exist in the transaction log.
     *
     * @param logEntry the log entry
     * @param listener a listener to apply some action before the entry is added
     */
    public void applyEntryFromLog(TransactionLogEntry logEntry, StateListenerBeforeApply listener) {
        if (logEntry.getTransactionType().isFileTransaction()) {
            files.applyEntryFromLog(logEntry, listener);
        } else {
            partitions.applyEntryFromLog(logEntry, listener);
        }
    }

    @Override
    public void clearSleeperTable() {
        files.clearTransactionLog();
        partitions.clearTransactionLog();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to create a state store backed by a transaction log.
     */
    public static class Builder {
        private TableProperties tableProperties;
        private TransactionLogStore filesLogStore;
        private TransactionLogStore partitionsLogStore;
        private TransactionBodyStore transactionBodyStore;
        private boolean updateLogBeforeAddTransaction = true;
        private TransactionLogSnapshotLoader filesSnapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private TransactionLogSnapshotLoader partitionsSnapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private Supplier<Instant> filesStateUpdateClock = Instant::now;
        private Supplier<Instant> partitionsStateUpdateClock = Instant::now;
        private DoubleSupplier randomJitterFraction = Math::random;
        private ThreadSleep retryWaiter = Thread::sleep;

        private Builder() {
        }

        /**
         * Sets the Sleeper table the state store is for, and configures the state store from the table properties.
         *
         * @param  tableProperties the table properties
         * @return                 the builder
         */
        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        /**
         * Sets the transaction log for file reference transactions.
         *
         * @param  filesLogStore the store holding the transaction log
         * @return               the builder
         */
        public Builder filesLogStore(TransactionLogStore filesLogStore) {
            this.filesLogStore = filesLogStore;
            return this;
        }

        /**
         * Sets the transaction log for partition transactions.
         *
         * @param  partitionsLogStore the store holding the transaction log
         * @return                    the builder
         */
        public Builder partitionsLogStore(TransactionLogStore partitionsLogStore) {
            this.partitionsLogStore = partitionsLogStore;
            return this;
        }

        /**
         * Sets the transaction body store.
         *
         * @param  transactionBodyStore the store
         * @return                      the builder
         */
        public Builder transactionBodyStore(TransactionBodyStore transactionBodyStore) {
            this.transactionBodyStore = transactionBodyStore;
            return this;
        }

        /**
         * Whether to update from the transaction log before adding a transaction. If more than one process is likely to
         * update the state store at the same time, this may be beneficial. If all or most transactions are added by a
         * single process, it may be better to avoid updating before every transaction to achieve higher throughput in
         * that process.
         * <p>
         * If another process has added a transaction when a process attempts to add a new one, it will update from the
         * transaction log and retry. Setting this to true avoids that retry, whereas setting this to false avoids the
         * need to update from the log for every new transaction.
         * <p>
         * Defaults to true.
         *
         * @param  updateLogBeforeAddTransaction whether to bring the local state up to date before adding a transaction
         * @return                               the builder
         */
        public Builder updateLogBeforeAddTransaction(boolean updateLogBeforeAddTransaction) {
            this.updateLogBeforeAddTransaction = updateLogBeforeAddTransaction;
            return this;
        }

        /**
         * Sets the configuration for exponential backoff during retries adding a transaction.
         *
         * @param  retryBackoff the backoff configuration
         * @return              the builder
         */
        public Builder retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            return this;
        }

        /**
         * Sets how to load snapshots for file references.
         *
         * @param  filesSnapshotLoader the loader
         * @return                     the builder
         */
        public Builder filesSnapshotLoader(TransactionLogSnapshotLoader filesSnapshotLoader) {
            this.filesSnapshotLoader = filesSnapshotLoader;
            return this;
        }

        /**
         * Sets how to load snapshots for partitions.
         *
         * @param  partitionsSnapshotLoader the loader
         * @return                          the builder
         */
        public Builder partitionsSnapshotLoader(TransactionLogSnapshotLoader partitionsSnapshotLoader) {
            this.partitionsSnapshotLoader = partitionsSnapshotLoader;
            return this;
        }

        /**
         * Sets the clock to use to determine when to check for new transactions or snapshots for file references. This
         * is used in tests to control the time that the state store sees.
         *
         * @param  filesStateUpdateClock the clock
         * @return                       the builder
         */
        public Builder filesStateUpdateClock(Supplier<Instant> filesStateUpdateClock) {
            this.filesStateUpdateClock = filesStateUpdateClock;
            return this;
        }

        /**
         * Sets the clock to use to determine when to check for new transactions or snapshots for partitions. This
         * is used in tests to control the time that the state store sees.
         *
         * @param  partitionsStateUpdateClock the clock
         * @return                            the builder
         */
        public Builder partitionsStateUpdateClock(Supplier<Instant> partitionsStateUpdateClock) {
            this.partitionsStateUpdateClock = partitionsStateUpdateClock;
            return this;
        }

        /**
         * Sets the supplier to produce a random amount of jitter as a fraction, when waiting to retry transactions.
         * Usually only overridden in tests.
         *
         * @param  randomJitterFraction the random jitter fraction supplier
         * @return                      the builder
         */
        public Builder randomJitterFraction(DoubleSupplier randomJitterFraction) {
            this.randomJitterFraction = randomJitterFraction;
            return this;
        }

        /**
         * Sets the method to sleep the current thread when waiting to retry transactions.
         * Implemented by <code>Thread.sleep</code>. Usually only overridden in tests.
         *
         * @param  retryWaiter the method to sleep the current thread
         * @return             the builder
         */
        public Builder retryWaiter(ThreadSleep retryWaiter) {
            this.retryWaiter = retryWaiter;
            return this;
        }

        public TransactionLogStateStore build() {
            return new TransactionLogStateStore(this);
        }
    }

}
