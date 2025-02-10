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
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * A transaction to add new partitions to the tree. This is done by setting child partitions for a partition that was
 * previously a leaf partition. This will turn the leaf partition into a parent and add new leaf partitions below it.
 */
public class SplitPartitionTransaction implements PartitionTransaction {

    private final Partition parent;
    private final List<Partition> newChildren;

    public SplitPartitionTransaction(Partition parent, List<Partition> newChildren) {
        this.parent = parent;
        this.newChildren = newChildren;
    }

    @Override
    public void validate(StateStorePartitions stateStorePartitions) throws StateStoreException {
        Partition existingParent = stateStorePartitions.byId(parent.getId())
                .orElseThrow(() -> new StateStoreException("Parent partition not found"));
        if (!existingParent.isLeafPartition()) {
            throw new StateStoreException("Parent should be a leaf partition");
        }

        Set<String> childIdsOnParent = new HashSet<>(parent.getChildPartitionIds());
        Set<String> newIds = newChildren.stream().map(Partition::getId).collect(toUnmodifiableSet());
        if (!childIdsOnParent.equals(newIds)) {
            throw new StateStoreException("Child partition IDs on parent do not match new children");
        }

        for (Partition child : newChildren) {
            if (stateStorePartitions.byId(child.getId()).isPresent()) {
                throw new StateStoreException("Child partition should not be present");
            }
            if (!child.getParentPartitionId().equals(parent.getId())) {
                throw new StateStoreException("Parent ID does not match on child: " + child.getId());
            }
            if (!child.isLeafPartition()) {
                throw new StateStoreException("Child should be a leaf partition: " + child.getId());
            }
        }
    }

    @Override
    public void apply(StateStorePartitions stateStorePartitions, Instant updateTime) {
        stateStorePartitions.put(parent);
        newChildren.forEach(stateStorePartitions::put);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, newChildren);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SplitPartitionTransaction)) {
            return false;
        }
        SplitPartitionTransaction other = (SplitPartitionTransaction) obj;
        return Objects.equals(parent, other.parent) && Objects.equals(newChildren, other.newChildren);
    }

    @Override
    public String toString() {
        return "SplitPartitionTransaction{parent=" + parent + ", newChildren=" + newChildren + "}";
    }
}
