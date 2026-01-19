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
package sleeper.core.statestore.transactionlog.transaction.impl;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Extends the partition tree by adding any number of splits as an atomic transaction. The updates can be provided as a
 * list of existing partitions that need to be updated, and a list of new partitions to add. The changes and resulting
 * partition tree will be validated.
 */
public class ExtendPartitionTreeTransaction implements PartitionTransaction {
    private final List<Partition> updatePartitions;
    private final List<Partition> newPartitions;

    public ExtendPartitionTreeTransaction(List<Partition> updatePartitions, List<Partition> newPartitions) {
        this.updatePartitions = updatePartitions;
        this.newPartitions = newPartitions;
    }

    /**
     * Commit this transaction directly to the state store without going to the commit queue. This will throw any
     * validation exceptions immediately, even if they wouldn't be as part of an asynchronous commit.
     *
     * @param  stateStore          the state store
     * @throws StateStoreException if the split is not valid or the update fails
     */
    public void synchronousCommit(StateStore stateStore) {
        stateStore.addPartitionsTransaction(AddTransactionRequest.withTransaction(this).build());
    }

    @Override
    public void validate(StateStorePartitions state, TableProperties tableProperties) throws StateStoreException {
        for (Partition partition : updatePartitions) {
            Optional<Partition> existing = state.byId(partition.getId());
            if (existing.isEmpty()) {
                throw new StateStoreException("Attempted to update a partition which does not exist: " + partition.getId());
            }
            if (!existing.get().isLeafPartition()) {
                throw new StateStoreException("Attempted to update a partition which has already been split: " + partition.getId());
            }
            if (partition.isLeafPartition()) {
                throw new StateStoreException("Attempted to update a partition without splitting it: " + partition.getId());
            }
        }
        for (Partition partition : newPartitions) {
            if (state.byId(partition.getId()).isPresent()) {
                throw new StateStoreException("Attempted to add a partition which already exists: " + partition.getId());
            }
        }
        Map<String, Partition> partitionById = state.all().stream().collect(toMap(Partition::getId, Function.identity()));
        updatePartitions.forEach(partition -> partitionById.put(partition.getId(), partition));
        newPartitions.forEach(partition -> partitionById.put(partition.getId(), partition));
        try {
            new PartitionTree(partitionById.values()).validate(tableProperties.getSchema());
        } catch (RuntimeException e) {
            throw new StateStoreException("Update results in invalid partition tree", e);
        }
    }

    @Override
    public void apply(StateStorePartitions state, Instant updateTime) {
        updatePartitions.forEach(state::put);
        newPartitions.forEach(state::put);
    }

    public List<Partition> getUpdatePartitions() {
        return updatePartitions;
    }

    public List<Partition> getNewPartitions() {
        return newPartitions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(updatePartitions, newPartitions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ExtendPartitionTreeTransaction)) {
            return false;
        }
        ExtendPartitionTreeTransaction other = (ExtendPartitionTreeTransaction) obj;
        return Objects.equals(updatePartitions, other.updatePartitions) && Objects.equals(newPartitions, other.newPartitions);
    }

    @Override
    public String toString() {
        return "ExtendPartitionTreeTransaction{updatePartitions=" + updatePartitions + ", newPartitions=" + newPartitions + "}";
    }
}
