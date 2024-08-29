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
import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.multipleWaitActions;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

/**
 * Gathers state for a state store backed by in-memory transaction logs. Helps with independent management of the
 * local state of the state store, by creating separate state store objects.
 */
public class InMemoryTransactionLogs {

    private final InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogSnapshots filesSnapshots = new InMemoryTransactionLogSnapshots();
    private final InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogSnapshots partitionsSnapshots = new InMemoryTransactionLogSnapshots();
    private final List<Duration> retryWaits = new ArrayList<>();
    private final Waiter retryWaiter;

    public InMemoryTransactionLogs() {
        retryWaiter = recordWaits(retryWaits);
    }

    private InMemoryTransactionLogs(Waiter extraWaiter) {
        retryWaiter = multipleWaitActions(recordWaits(retryWaits), extraWaiter);
    }

    /**
     * Creates an instance of this class that will record the waits during transaction retries in an additional list.
     * 
     * @param  retryWaits the list to record retry waits in
     * @return            an instance of this class
     */
    public static InMemoryTransactionLogs recordRetryWaits(List<Duration> retryWaits) {
        return new InMemoryTransactionLogs(recordWaits(retryWaits));
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
                .retryBackoff(new ExponentialBackoffWithJitter(
                        TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                        constantJitterFraction(0.5), retryWaiter));
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

    public List<Duration> getRetryWaits() {
        return retryWaits;
    }
}
