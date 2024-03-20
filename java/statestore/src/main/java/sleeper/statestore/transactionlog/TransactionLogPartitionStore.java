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
package sleeper.statestore.transactionlog;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.statestore.transactionlog.transactions.PartitionTransaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

class TransactionLogPartitionStore implements PartitionStore {

    private final TableProperties tableProperties;
    private final TransactionLogStore logStore;

    TransactionLogPartitionStore(TableProperties tableProperties, TransactionLogStore logStore) {
        this.tableProperties = tableProperties;
        this.logStore = logStore;
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
    }

    @Override
    public void clearPartitionData() {
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        return partitions().collect(toUnmodifiableList());
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        return partitions().filter(Partition::isLeafPartition).collect(toUnmodifiableList());
    }

    private Stream<Partition> partitions() {
        Map<String, Partition> partitionById = new HashMap<>();
        logStore.readAllTransactions(PartitionTransaction.class)
                .forEach(transaction -> transaction.apply(partitionById));
        return partitionById.values().stream();
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(tableProperties.getSchema(), Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        logStore.addTransaction(new InitialisePartitionsTransaction(partitions));
    }

}
