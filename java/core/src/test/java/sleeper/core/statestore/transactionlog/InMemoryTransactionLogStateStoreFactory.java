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
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ThreadSleep;
import sleeper.core.util.ThreadSleepTestHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;

/**
 * Gathers state for a state store backed by in-memory transaction logs. Helps with independent management of the
 * local state of the state store, by creating separate state store objects.
 */
public class InMemoryTransactionLogStateStoreFactory {

    private final InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogSnapshots filesSnapshots = new InMemoryTransactionLogSnapshots();
    private final InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogSnapshots partitionsSnapshots = new InMemoryTransactionLogSnapshots();
    private final InMemoryTransactionBodyStore transactionBodyStore;
    private final List<Duration> retryWaits;
    private final ThreadSleep retryWaiter;

    public InMemoryTransactionLogStateStoreFactory() {
        this(new InMemoryTransactionBodyStore());
    }

    public InMemoryTransactionLogStateStoreFactory(InMemoryTransactionBodyStore transactionBodyStore) {
        this(transactionBodyStore, new ArrayList<>());
    }

    private InMemoryTransactionLogStateStoreFactory(InMemoryTransactionBodyStore transactionBodyStore, List<Duration> retryWaits) {
        this(transactionBodyStore, retryWaits, ThreadSleepTestHelper.recordWaits(retryWaits));
    }

    private InMemoryTransactionLogStateStoreFactory(InMemoryTransactionBodyStore transactionBodyStore, List<Duration> retryWaits, ThreadSleep retryWaiter) {
        this.transactionBodyStore = transactionBodyStore;
        this.retryWaits = retryWaits;
        this.retryWaiter = retryWaiter;
    }

    /**
     * Creates an instance of this class that will record the waits during transaction retries in a given list.
     *
     * @param  retryWaits the list to record retry waits in
     * @return            an instance of this class
     */
    public static InMemoryTransactionLogStateStoreFactory recordRetryWaits(InMemoryTransactionBodyStore transactionBodyStore, List<Duration> retryWaits) {
        return new InMemoryTransactionLogStateStoreFactory(transactionBodyStore, retryWaits);
    }

    /**
     * Creates an instance of this class that will record the waits during transaction retries in a given list.
     *
     * @param  retryWaits the list to record retry waits in
     * @return            an instance of this class
     */
    public static InMemoryTransactionLogStateStoreFactory recordRetryWaits(List<Duration> retryWaits) {
        return new InMemoryTransactionLogStateStoreFactory(new InMemoryTransactionBodyStore(), retryWaits);
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
                .schema(schema)
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

    public List<Duration> getRetryWaits() {
        return retryWaits;
    }
}
