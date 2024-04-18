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

import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;

public class TransactionLogHeadLoader<T> {
    private final TableStatus sleeperTable;
    private final int maxAddTransactionAttempts;
    private final ExponentialBackoffWithJitter retryBackoff;
    private final Class<? extends StateStoreTransaction<T>> transactionType;
    private final T state;

    TransactionLogHeadLoader(
            TableStatus sleeperTable, int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff,
            Class<? extends StateStoreTransaction<T>> transactionType, T state) {
        this.sleeperTable = sleeperTable;
        this.maxAddTransactionAttempts = maxAddTransactionAttempts;
        this.retryBackoff = retryBackoff;
        this.transactionType = transactionType;
        this.state = state;
    }

    public static TransactionLogHeadLoader<StateStoreFiles> forFiles(
            TableStatus sleeperTable, int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff) {
        return new TransactionLogHeadLoader<StateStoreFiles>(
                sleeperTable, maxAddTransactionAttempts, retryBackoff,
                FileReferenceTransaction.class, new StateStoreFiles());
    }

    public static TransactionLogHeadLoader<StateStorePartitions> forPartitions(
            TableStatus sleeperTable, int maxAddTransactionAttempts, ExponentialBackoffWithJitter retryBackoff) {
        return new TransactionLogHeadLoader<StateStorePartitions>(
                sleeperTable, maxAddTransactionAttempts, retryBackoff,
                PartitionTransaction.class, new StateStorePartitions());
    }

    public T getState(TransactionLogStore store) throws StateStoreException {
        TransactionLogHead<T> head = head(store);
        head.update();
        return head.state();
    }

    public long getLastTransactionNumber(TransactionLogStore store) throws StateStoreException {
        TransactionLogHead<T> head = head(store);
        head.update();
        return head.lastTransactionNumber();
    }

    public TransactionLogHead<T> head(TransactionLogStore store) {
        return new TransactionLogHead<T>(sleeperTable, store, maxAddTransactionAttempts, retryBackoff, transactionType, state);
    }
}
