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
package sleeper.core.statestore.testutils;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ThreadSleep;
import sleeper.core.util.ThreadSleepTestHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;

/**
 * Gathers state for a state store backed by in-memory transaction logs. Helps with independent management of the
 * local state of the state store, by creating separate state store objects.
 */
public class InMemoryTransactionLogs {

    private final InMemoryTransactionLogStore filesLogStore;
    private final InMemoryTransactionLogSnapshots filesSnapshots;
    private final InMemoryTransactionLogStore partitionsLogStore;
    private final InMemoryTransactionLogSnapshots partitionsSnapshots;
    private final InMemoryTransactionBodyStore transactionBodyStore;
    private final List<Duration> retryWaits;
    private final ThreadSleep retryWaiter;

    public InMemoryTransactionLogs() {
        this(builder());
    }

    private InMemoryTransactionLogs(Builder builder) {
        filesLogStore = builder.filesLogStore;
        filesSnapshots = builder.filesSnapshots;
        partitionsLogStore = builder.partitionsLogStore;
        partitionsSnapshots = builder.partitionsSnapshots;
        transactionBodyStore = builder.transactionBodyStore;
        retryWaits = builder.retryWaits;
        retryWaiter = builder.retryWaiter;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an instance of this class that will record the waits during transaction retries in a given list.
     *
     * @param  transactionBodyStore the store of large transactions
     * @param  retryWaits           the list to record retry waits in
     * @return                      an instance of this class
     */
    public static InMemoryTransactionLogs recordRetryWaits(InMemoryTransactionBodyStore transactionBodyStore, List<Duration> retryWaits) {
        return builder().transactionBodyStore(transactionBodyStore).retryWaits(retryWaits).build();
    }

    /**
     * Creates an instance of this class that will record the waits during transaction retries in a given list.
     *
     * @param  retryWaits the list to record retry waits in
     * @return            an instance of this class
     */
    public static InMemoryTransactionLogs recordRetryWaits(List<Duration> retryWaits) {
        return builder().retryWaits(retryWaits).build();
    }

    /**
     * Creates a builder for a state store backed by the transaction logs held in this class.
     *
     * @param  sleeperTable the status of the table the state store is for
     * @param  schema       the schema of the table the state store is for
     * @return              the builder
     */
    public TransactionLogStateStore.Builder stateStoreBuilder(TableStatus sleeperTable, Schema schema) {
        return TransactionLogStateStore.builder()
                .sleeperTable(sleeperTable)
                .filesLogStore(filesLogStore)
                .filesSnapshotLoader(filesSnapshots)
                .partitionsLogStore(partitionsLogStore)
                .partitionsSnapshotLoader(partitionsSnapshots)
                .maxAddTransactionAttempts(10)
                .transactionBodyStore(transactionBodyStore)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                        constantJitterFraction(0.5), retryWaiter));
    }

    /**
     * Fakes creating snapshots of the current state of the transaction logs.
     *
     * @param tableStatus the Sleeper table status
     */
    public void createSnapshots(TableStatus tableStatus) {
        InMemoryTransactionLogSnapshotSetup setup = new InMemoryTransactionLogSnapshotSetup(tableStatus, filesLogStore, partitionsLogStore, transactionBodyStore);
        filesSnapshots.setLatestSnapshot(setup.createFilesSnapshot(filesLogStore.getLastTransactionNumber()));
        partitionsSnapshots.setLatestSnapshot(setup.createPartitionsSnapshot(partitionsLogStore.getLastTransactionNumber()));
    }

    public InMemoryTransactionLogStore getFilesLogStore() {
        return filesLogStore;
    }

    public InMemoryTransactionLogSnapshots getFilesSnapshots() {
        return filesSnapshots;
    }

    public InMemoryTransactionLogStore getPartitionsLogStore() {
        return partitionsLogStore;
    }

    public InMemoryTransactionLogSnapshots getPartitionsSnapshots() {
        return partitionsSnapshots;
    }

    public InMemoryTransactionBodyStore getTransactionBodyStore() {
        return transactionBodyStore;
    }

    /**
     * Gets the last transaction from the files transaction log store.
     *
     * @param  tableProperties the table properties
     * @return                 the file reference transaction
     */
    public FileReferenceTransaction getLastFilesTransaction(TableProperties tableProperties) {
        return (FileReferenceTransaction) filesLogStore.getLastEntry()
                .getTransactionOrLoadFromPointer(tableProperties.get(TABLE_ID), transactionBodyStore);
    }

    public List<Duration> getRetryWaits() {
        return retryWaits;
    }

    /**
     * A builder to create in memory transaction logs.
     */
    public static class Builder {

        private InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
        private InMemoryTransactionLogSnapshots filesSnapshots = new InMemoryTransactionLogSnapshots();
        private InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
        private InMemoryTransactionLogSnapshots partitionsSnapshots = new InMemoryTransactionLogSnapshots();
        private InMemoryTransactionBodyStore transactionBodyStore = new InMemoryTransactionBodyStore();
        private List<Duration> retryWaits = new ArrayList<>();
        private ThreadSleep retryWaiter = ThreadSleepTestHelper.recordWaits(retryWaits);

        private Builder() {
        }

        /**
         * Sets the store of the log of transactions against files.
         *
         * @param  filesLogStore the store
         * @return               this builder
         */
        public Builder filesLogStore(InMemoryTransactionLogStore filesLogStore) {
            this.filesLogStore = filesLogStore;
            return this;
        }

        /**
         * Sets the store of snapshots of the state of files.
         *
         * @param  filesSnapshots the store
         * @return                this builder
         */
        public Builder filesSnapshots(InMemoryTransactionLogSnapshots filesSnapshots) {
            this.filesSnapshots = filesSnapshots;
            return this;
        }

        /**
         * Sets the store of the log of transactions against partitions.
         *
         * @param  partitionsLogStore the store
         * @return                    this builder
         */
        public Builder partitionsLogStore(InMemoryTransactionLogStore partitionsLogStore) {
            this.partitionsLogStore = partitionsLogStore;
            return this;
        }

        /**
         * Sets the store of snapshots of the state of partitions.
         *
         * @param  partitionsSnapshots the store
         * @return                     this builder
         */
        public Builder partitionsSnapshots(InMemoryTransactionLogSnapshots partitionsSnapshots) {
            this.partitionsSnapshots = partitionsSnapshots;
            return this;
        }

        /**
         * Sets the store of large transactions that are not held directly in a log entry.
         *
         * @param  transactionBodyStore the store
         * @return                      this builder
         */
        public Builder transactionBodyStore(InMemoryTransactionBodyStore transactionBodyStore) {
            this.transactionBodyStore = transactionBodyStore;
            return this;
        }

        /**
         * Sets the list of wait durations to be written to when the state store waits during a retry. This will be used
         * for testing purposes instead of actually waiting.
         *
         * @param  retryWaits the list to record wait durations
         * @return            this builder
         */
        public Builder retryWaits(List<Duration> retryWaits) {
            this.retryWaits = retryWaits;
            return retryWaiter(ThreadSleepTestHelper.recordWaits(retryWaits));
        }

        /**
         * Sets the behaviour to use when the state store waits during a retry. This will be used for testing purposes
         * instead of actually waiting.
         *
         * @param  retryWaiter the behaviour
         * @return             this builder
         */
        public Builder retryWaiter(ThreadSleep retryWaiter) {
            this.retryWaiter = retryWaiter;
            return this;
        }

        public InMemoryTransactionLogs build() {
            return new InMemoryTransactionLogs(this);
        }
    }
}
