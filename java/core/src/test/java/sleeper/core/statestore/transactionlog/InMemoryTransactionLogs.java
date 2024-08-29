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

import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;

/**
 * Gathers state for a state store backed by in-memory transaction logs. Helps with independent management of the
 * local state of the state store, by creating separate state store objects.
 */
public class InMemoryTransactionLogs {

    private final InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogSnapshots filesSnapshots = new InMemoryTransactionLogSnapshots();
    private final InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogSnapshots partitionsSnapshots = new InMemoryTransactionLogSnapshots();

    /**
     * Creates a builder for a state store backed by the transaction logs held in this class.
     *
     * @param  retryWaiter behaviour for waiting on retries, see ExponentialBackoffWithJitterTestHelper
     * @return             the builder
     */
    public TransactionLogStateStore.Builder stateStoreBuilder(TableStatus sleeperTable, Schema schema, Waiter retryWaiter) {
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
}
