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
package sleeper.core.statestore.testutils;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An in-memory partition store implementation backed by a list.
 */
public class InMemoryPartitionStore implements PartitionStore {

    private Schema schema;
    private List<Partition> partitions = List.of();

    public InMemoryPartitionStore(List<Partition> partitions) {
        initialise(partitions);
    }

    public InMemoryPartitionStore(Schema schema) {
        this.schema = schema;
    }

    /**
     * Creates an in-memory partition store with a single root partition. This will be derived from the given schema.
     *
     * @param  schema the schema
     * @return        the store
     */
    public static PartitionStore withSinglePartition(Schema schema) {
        InMemoryPartitionStore store = new InMemoryPartitionStore(schema);
        store.initialise();
        return store;
    }

    @Override
    public List<Partition> getAllPartitions() {
        return partitions;
    }

    @Override
    public List<Partition> getLeafPartitions() {
        return partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void initialise() {
        if (schema == null) {
            throw new UnsupportedOperationException("Not supported without schema");
        } else {
            initialise(new PartitionsFromSplitPoints(schema, List.of()).construct());
        }
    }

    @Override
    public void initialise(List<Partition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public void clearPartitionData() {
        partitions = List.of();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(
            Partition parent, Partition left, Partition right) throws StateStoreException {
        PartitionTree oldTree = new PartitionTree(partitions);
        if (!oldTree.getPartition(parent.getId()).isLeafPartition()) {
            throw new StateStoreException("Partition has already been split: " + parent.getId());
        }
        if (!parent.getChildPartitionIds().equals(List.of(left.getId(), right.getId()))) {
            throw new StateStoreException("Child partition IDs do not match: " + parent.getChildPartitionIds());
        }
        validateChild(parent, left);
        validateChild(parent, right);
        partitions = Stream.concat(
                partitions.stream().filter(partition -> !Objects.equals(partition.getId(), parent.getId())),
                Stream.of(parent, left, right))
                .collect(Collectors.toUnmodifiableList());
    }

    private static void validateChild(Partition parent, Partition child) throws StateStoreException {
        if (!child.isLeafPartition()) {
            throw new StateStoreException("Child partition is not a leaf: " + child.getId());
        }
        if (!child.getParentPartitionId().equals(parent.getId())) {
            throw new StateStoreException("Child partition parent ID does not match: " + child.getParentPartitionId());
        }
    }
}
