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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Atomically splits a partition to create child partitions. This is done by setting child partitions for a partition
 * that was previously a leaf partition.
 * <p>
 * The new partitions must be leaf partitions. The parent partition must already exist, and will be replaced with the
 * updated version. The new version must refer to the new leaf partitions as children.
 */
public class SplitPartitionTransaction implements PartitionTransaction {

    private final Partition parent;
    private final List<Partition> newChildren;

    public SplitPartitionTransaction(Partition parent, List<Partition> newChildren) {
        this.parent = parent;
        this.newChildren = newChildren;
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
    public void validate(StateStorePartitions stateStorePartitions, TableProperties tableProperties) throws StateStoreException {
        Partition existingParent = stateStorePartitions.byId(parent.getId())
                .orElseThrow(() -> new StateStoreException("Parent partition not found"));
        if (!existingParent.isLeafPartition()) {
            throw new StateStoreException("Parent should be a leaf partition");
        }
        if (parent.isLeafPartition()) {
            throw new StateStoreException("Parent should not be a leaf partition after split");
        }

        if (!Objects.equals(parent.getChildPartitionIds(), checkChildPartitionIdsInSplitOrder(tableProperties.getSchema()))) {
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

    private List<String> checkChildPartitionIdsInSplitOrder(Schema schema) throws StateStoreException {
        if (newChildren.size() != 2) {
            throw new StateStoreException("Expected 2 child partitions, left and right of split point, found " + newChildren.size());
        }
        if (schema.getRowKeyFields().size() <= parent.getDimension()) {
            throw new StateStoreException("No row key at parent partition dimension " + parent.getDimension());
        }
        Field splitField = schema.getRowKeyFields().get(parent.getDimension());
        Partition child1 = newChildren.get(0);
        Partition child2 = newChildren.get(1);
        Range range1 = child1.getRegion().getRange(splitField.getName());
        Range range2 = child2.getRegion().getRange(splitField.getName());
        PrimitiveType type = (PrimitiveType) splitField.getType();
        if (type.compare(range1.getMax(), range2.getMin()) == 0) {
            return List.of(child1.getId(), child2.getId());
        } else if (type.compare(range1.getMin(), range2.getMax()) == 0) {
            return List.of(child2.getId(), child1.getId());
        } else {
            throw new StateStoreException("Child partitions do not meet at a split point on dimension " + parent.getDimension() + " set in the parent");
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
