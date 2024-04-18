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
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;

import java.nio.file.Path;

public class InMemoryTransactionLogPartitionsSnapshot implements TransactionLogSnapshot<StateStorePartitions> {
    private final TransactionLogStoreLoader<StateStorePartitions> storeLoader;
    private final TransactionLogStore logStore;

    public static InMemoryTransactionLogPartitionsSnapshot fromLogStore(
            Schema schema, TableStatus sleeperTable, TransactionLogStore logStore,
            int maxAddTransactionAttempts, ExponentialBackoffWithJitter backoffWithJitter) {
        return new InMemoryTransactionLogPartitionsSnapshot(schema,
                TransactionLogStoreLoader.forPartitions(
                        sleeperTable, maxAddTransactionAttempts, backoffWithJitter),
                logStore);
    }

    InMemoryTransactionLogPartitionsSnapshot(Schema schema, TransactionLogStoreLoader<StateStorePartitions> storeLoader, TransactionLogStore logStore) {
        this.storeLoader = storeLoader;
        this.logStore = logStore;
    }

    public StateStorePartitions state() throws StateStoreException {
        return storeLoader.getState(logStore);
    }

    public long lastTransactionNumber() throws StateStoreException {
        return storeLoader.getLastTransactionNumber(logStore);
    }

    public void save(Path tempDir) {
    }
}
