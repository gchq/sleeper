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
package sleeper.core.statestore.transactionlog.transactions;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.TransactionLogStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StateStoreState {

    private final TransactionLogStore logStore;
    private final Map<String, Partition> partitionById = new HashMap<>();
    private final StateStoreFiles files = new StateStoreFiles();
    private long lastTransactionNumber = 0;

    public StateStoreState(TransactionLogStore logStore) {
        this.logStore = logStore;
    }

    public void addTransaction(StateStoreTransaction transaction) throws StateStoreException {
        update();
        transaction.validate(this);
        long transactionNumber = lastTransactionNumber + 1;
        logStore.addTransaction(transaction, transactionNumber);
        transaction.apply(this);
        lastTransactionNumber = transactionNumber;
    }

    public void update() {
        logStore.readTransactionsAfter(lastTransactionNumber).forEach(transaction -> {
            transaction.apply(this);
            lastTransactionNumber++;
        });
    }

    public Collection<Partition> partitions() {
        return partitionById.values();
    }

    Map<String, Partition> partitionById() {
        return partitionById;
    }

    public StateStoreFiles files() {
        return files;
    }

}
