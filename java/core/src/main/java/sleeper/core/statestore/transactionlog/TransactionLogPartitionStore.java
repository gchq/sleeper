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

import sleeper.core.partition.Partition;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearPartitionsTransaction;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * A partition store backed by a log of transactions. Part of {@link TransactionLogStateStore}.
 */
class TransactionLogPartitionStore implements PartitionStore {

    private final TransactionLogHead<StateStorePartitions> head;
    private Supplier<Instant> timeSupplier = Instant::now;

    TransactionLogPartitionStore(TransactionLogHead<StateStorePartitions> head) {
        this.head = head;
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitions().all().stream().toList();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitions().all().stream().filter(Partition::isLeafPartition).toList();
    }

    @Override
    public Partition getPartition(String partitionId) throws StateStoreException {
        return partitions().byId(partitionId)
                .orElseThrow(() -> new StateStoreException("Partition not found: " + partitionId));
    }

    @Override
    public void fixPartitionUpdateTime(Instant time) {
        timeSupplier = () -> time;
    }

    /**
     * Updates the local state from the transaction log.
     *
     * @throws StateStoreException thrown if there's any failure reading transactions or applying them to the state
     */
    public void updateFromLog() throws StateStoreException {
        head.update();
    }

    @Override
    public void addPartitionsTransaction(AddTransactionRequest request) {
        head.addTransaction(timeSupplier.get(), request);
    }

    void applyEntryFromLog(TransactionLogEntry logEntry, StateListenerBeforeApply listener) {
        head.applyTransactionUpdatingIfNecessary(logEntry, listener);
    }

    void clearTransactionLog() {
        head.clearTransactionLog(ClearPartitionsTransaction.create(), timeSupplier.get());
    }

    private StateStorePartitions partitions() throws StateStoreException {
        head.update();
        return head.state();
    }

}
