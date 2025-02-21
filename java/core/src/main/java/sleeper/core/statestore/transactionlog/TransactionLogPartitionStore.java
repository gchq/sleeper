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

import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitPartitionTransaction;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A partition store backed by a log of transactions. Part of {@link TransactionLogStateStore}.
 */
class TransactionLogPartitionStore implements PartitionStore {

    private final Schema schema;
    private final TransactionLogHead<StateStorePartitions> head;
    private Clock clock = Clock.systemUTC();

    TransactionLogPartitionStore(Schema schema, TransactionLogHead<StateStorePartitions> head) {
        this.schema = schema;
        this.head = head;
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        head.addTransaction(clock.instant(), new SplitPartitionTransaction(splitPartition, List.of(newPartition1, newPartition2)));
    }

    @Override
    public void clearPartitionData() throws StateStoreException {
        head.addTransaction(clock.instant(), new InitialisePartitionsTransaction(List.of()));
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitions().all().stream().toList();
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitions().all().stream().filter(Partition::isLeafPartition).collect(toUnmodifiableList());
    }

    @Override
    public Partition getPartition(String partitionId) throws StateStoreException {
        return partitions().byId(partitionId)
                .orElseThrow(() -> new StateStoreException("Partition not found: " + partitionId));
    }

    @Override
    public void initialise() throws StateStoreException {
        head.addTransaction(clock.instant(), InitialisePartitionsTransaction.singlePartition(schema));
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        head.addTransaction(clock.instant(), new InitialisePartitionsTransaction(partitions));
    }

    @Override
    public void fixPartitionUpdateTime(Instant time) {
        clock = Clock.fixed(time, ZoneId.of("UTC"));
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
        head.addTransaction(clock.instant(), request);
    }

    void applyEntryFromLog(TransactionLogEntry logEntry, StateListenerBeforeApply listener) {
        head.applyTransactionUpdatingIfNecessary(logEntry, listener);
    }

    private StateStorePartitions partitions() throws StateStoreException {
        head.update();
        return head.state();
    }

}
