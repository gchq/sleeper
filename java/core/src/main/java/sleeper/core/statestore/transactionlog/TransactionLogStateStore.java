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

import sleeper.core.schema.Schema;
import sleeper.core.statestore.DelegatingStateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

import java.time.Duration;
import java.time.Instant;
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
                .sleeperTable(builder.sleeperTable)
                .transactionBodyStore(builder.transactionBodyStore)
                .updateLogBeforeAddTransaction(builder.updateLogBeforeAddTransaction)
                .maxAddTransactionAttempts(builder.maxAddTransactionAttempts)
                .timeBetweenSnapshotChecks(builder.timeBetweenSnapshotChecks)
                .timeBetweenTransactionChecks(builder.timeBetweenTransactionChecks)
                .minTransactionsAheadToLoadSnapshot(builder.minTransactionsAheadToLoadSnapshot)
                .retryBackoff(builder.retryBackoff));
    }

    private TransactionLogStateStore(Builder builder, TransactionLogHead.Builder<?> headBuilder) {
        this(builder.schema,
                headBuilder.forFiles()
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

    private TransactionLogStateStore(Schema schema, TransactionLogHead<StateStoreFiles> filesHead, TransactionLogHead<StateStorePartitions> partitionsHead) {
        this(new TransactionLogFileReferenceStore(filesHead),
                new TransactionLogPartitionStore(schema, partitionsHead));
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
     * Adds a transaction to the transaction log. The transaction may or may not already be held in S3. If it is already
     * held in S3, we don't need to write it to S3 again.
     *
     * @param request the request
     */
    public void addTransaction(AddTransactionRequest request) {
        if (request.getTransaction() instanceof FileReferenceTransaction) {
            files.addTransaction(request);
        } else if (request.getTransaction() instanceof PartitionTransaction) {
            partitions.addTransaction(request);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to create a state store backed by a transaction log.
     */
    public static class Builder {
        private TableStatus sleeperTable;
        private Schema schema;
        private TransactionLogStore filesLogStore;
        private TransactionLogStore partitionsLogStore;
        private TransactionBodyStore transactionBodyStore;
        private long minTransactionsAheadToLoadSnapshot = DEFAULT_MIN_TRANSACTIONS_AHEAD_TO_LOAD_SNAPSHOT;
        private boolean updateLogBeforeAddTransaction = true;
        private int maxAddTransactionAttempts = DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS;
        private ExponentialBackoffWithJitter retryBackoff = new ExponentialBackoffWithJitter(DEFAULT_RETRY_WAIT_RANGE);
        private TransactionLogSnapshotLoader filesSnapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private TransactionLogSnapshotLoader partitionsSnapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private Duration timeBetweenSnapshotChecks = DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS;
        private Duration timeBetweenTransactionChecks = DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS;
        private Supplier<Instant> filesStateUpdateClock = Instant::now;
        private Supplier<Instant> partitionsStateUpdateClock = Instant::now;

        private Builder() {
        }

        /**
         * Sets the Sleeper table the state store is for. Used in logging.
         *
         * @param  sleeperTable the table status
         * @return              the builder
         */
        public Builder sleeperTable(TableStatus sleeperTable) {
            this.sleeperTable = sleeperTable;
            return this;
        }

        /**
         * Sets the schema of the Sleeper table. Used to initialise partitions.
         *
         * @param  schema the schema
         * @return        the builder
         */
        public Builder schema(Schema schema) {
            this.schema = schema;
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
         * The minimum number of transactions ahead that a snapshot must be before we should load it. If a snapshot
         * exists that is fewer than this many transactions ahead of the local state, we will read the transactions from
         * the log instead of loading the snapshot.
         *
         * @param  minTransactionsAheadToLoadSnapshot the minimum number of transactions ahead to load a snapshot
         * @return                                    the builder
         */
        public Builder minTransactionsAheadToLoadSnapshot(long minTransactionsAheadToLoadSnapshot) {
            this.minTransactionsAheadToLoadSnapshot = minTransactionsAheadToLoadSnapshot;
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
         * Sets the maximum number of attempts when retrying adding a transaction due to a conflict.
         *
         * @param  maxAddTransactionAttempts the number of attempts
         * @return                           the builder
         */
        public Builder maxAddTransactionAttempts(int maxAddTransactionAttempts) {
            this.maxAddTransactionAttempts = maxAddTransactionAttempts;
            return this;
        }

        /**
         * Sets the configuration for exponential backoff during retries adding a transaction.
         *
         * @param  retryBackoff the backoff configuration
         * @return              the builder
         */
        public Builder retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
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
         * Sets the amount of time to wait after checking for a new snapshot before checking again. This can avoid
         * repeatedly querying an index of snapshots.
         *
         * @param  timeBetweenSnapshotChecks the wait time between checks for a snapshot
         * @return                           the builder
         */
        public Builder timeBetweenSnapshotChecks(Duration timeBetweenSnapshotChecks) {
            this.timeBetweenSnapshotChecks = timeBetweenSnapshotChecks;
            return this;
        }

        /**
         * Sets the amount of time to wait after checking for new transactions before checking again. This is only
         * applied when querying the state store, not during an update. This should not be used if you have multiple
         * instances of the state store that you expect to be up to date with one another immediately.
         *
         * @param  timeBetweenTransactionChecks the wait time between checks for new transactions
         * @return                              the builder
         */
        public Builder timeBetweenTransactionChecks(Duration timeBetweenTransactionChecks) {
            this.timeBetweenTransactionChecks = timeBetweenTransactionChecks;
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

        public TransactionLogStateStore build() {
            return new TransactionLogStateStore(this);
        }
    }

}
