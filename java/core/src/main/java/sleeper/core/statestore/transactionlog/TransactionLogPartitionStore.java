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
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.SplitPartitionTransaction;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

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
        return partitions().collect(toUnmodifiableList());
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitions().filter(Partition::isLeafPartition).collect(toUnmodifiableList());
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
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

    void addTransaction(AddTransactionRequest request) {
        head.addTransaction(clock.instant(), request);
    }

    private Stream<Partition> partitions() throws StateStoreException {
        head.update();
        return head.state().all().stream();
    }

}
