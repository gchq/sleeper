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
 * A test helper to create state stores based on transaction logs held in memory.
 */
public class InMemoryTransactionLogStateStoreTestHelper {

    private InMemoryTransactionLogStateStoreTestHelper() {
    }

    /**
     * Creates a builder for a state store backed by an in-memory transaction log.
     *
     * @param  sleeperTable the Sleeper table status
     * @param  schema       the Sleeper table schema
     * @param  retryWaiter  behaviour for waiting on retries, see ExponentialBackoffWithJitterTestHelper
     * @return              the builder
     */
    public static TransactionLogStateStore.Builder inMemoryTransactionLogStateStoreBuilder(TableStatus sleeperTable, Schema schema, Waiter retryWaiter) {
        return TransactionLogStateStore.builder()
                .sleeperTable(sleeperTable)
                .schema(schema)
                .filesLogStore(new InMemoryTransactionLogStore())
                .filesSnapshotLoader(new InMemoryTransactionLogSnapshots())
                .partitionsLogStore(new InMemoryTransactionLogStore())
                .partitionsSnapshotLoader(new InMemoryTransactionLogSnapshots())
                .maxAddTransactionAttempts(10)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                        constantJitterFraction(0.5), retryWaiter));
    }

}
