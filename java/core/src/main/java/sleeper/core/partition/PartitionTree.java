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
package sleeper.core.partition;

import sleeper.core.key.Key;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a tree of partitions. It can be used to traverse or query the tree, e.g. to find all ancestors of a
 * partition, partitions that are either parents of the partition, or grandparents, or great-grandparents.
 */
public class PartitionTree {
    private final Map<String, Partition> idToPartition;
    private final Partition rootPartition;

    public PartitionTree(List<Partition> partitions) {
        this.idToPartition = new HashMap<>();
        partitions.forEach(p -> this.idToPartition.put(p.getId(), p));
        List<Partition> rootPartitions = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList());
        // There should be exactly one root partition.
        if (rootPartitions.size() != 1) {
            throw new IllegalArgumentException("There should be exactly one root partition, found " + rootPartitions.size());
        }
        this.rootPartition = rootPartitions.get(0);
    }

    /**
     * Retrieve IDs of partitions that were split from a given parent partition.
     *
     * @param  partitionId              the ID of the parent partition
     * @return                          the IDs of the child partitions
     * @throws IllegalArgumentException if the parent partition does not exist
     */
    public List<String> getChildIds(String partitionId) throws IllegalArgumentException {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }
        return idToPartition.get(partitionId).getChildPartitionIds();
    }

    /**
     * Retrieve IDs of all partitions that were split to result in the given partition. Starts with the most recent
     * parent, and includes all ancestors of that parent ending with the root partition.
     *
     * @param  partitionId the ID of the partition to find ancestors of
     * @return             all IDs of the partition's ancestors
     */
    public List<String> getAllAncestorIds(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }

        List<String> ancestors = new ArrayList<>();
        String parent = idToPartition.get(partitionId).getParentPartitionId();
        while (null != parent) {
            ancestors.add(parent);
            parent = null == idToPartition.get(parent) ? null : idToPartition.get(parent).getParentPartitionId();
        }
        return ancestors;
    }

    /**
     * Retrieve all partitions that were split to result in the given partition. Starts with the most recent parent, and
     * includes all ancestors of that parent ending with the root partition.
     *
     * @param  partitionId the ID of the partition to find ancestors of
     * @return             all of the partition's ancestors
     */
    public List<Partition> getAllAncestors(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }

        List<Partition> ancestors = new ArrayList<>();
        String parentId = idToPartition.get(partitionId).getParentPartitionId();
        while (null != parentId) {
            ancestors.add(idToPartition.get(parentId));
            parentId = idToPartition.get(parentId).getParentPartitionId();
        }
        return ancestors;
    }

    /**
     * Retrieve a partition by its unique ID.
     *
     * @param  partitionId the ID of the partition
     * @return             the partition
     */
    public Partition getPartition(String partitionId) {
        return idToPartition.get(partitionId);
    }

    public List<Partition> getAllPartitions() {
        return List.copyOf(idToPartition.values());
    }

    /**
     * Retrieve the leaf partition that includes a given key.
     *
     * @param  schema schema of the Sleeper table
     * @param  key    values for the key to find the leaf partition for
     * @return        the leaf partition
     */
    public Partition getLeafPartition(Schema schema, Key key) {
        // Sanity check key is of the correct length
        if (key.size() != schema.getRowKeyFields().size()) {
            throw new IllegalArgumentException("Key must match the row key fields from the schema (key was "
                    + key + ", schema has row key fields " + schema.getRowKeyFields() + ")");
        }
        // If root partition is a leaf partition then key must be in it
        if (rootPartition.isLeafPartition()) {
            return rootPartition;
        }

        return descend(schema, rootPartition, key);
    }

    private Partition descend(Schema schema, Partition currentNode, Key key) {
        // Get child partitions
        List<String> childPartitionIds = currentNode.getChildPartitionIds();
        List<Partition> childPartitions = new ArrayList<>();
        childPartitionIds.forEach(c -> childPartitions.add(idToPartition.get(c)));

        // Which child is the key in?
        Partition child = null;
        for (Partition partition : childPartitions) {
            if (partition.isRowKeyInPartition(schema, key)) {
                child = partition;
                break;
            }
        }
        if (null == child) {
            throw new IllegalArgumentException("Found key that was not in any of the child partitions: key " + key
                    + ", child partitions " + childPartitions);
        }
        if (child.isLeafPartition()) {
            return child;
        }
        return descend(schema, child, key);
    }

    public Partition getRootPartition() {
        return rootPartition;
    }

    /**
     * Retrieve the partition whose region includes both keys. Finds the nearest partition to each of the keys, with the
     * fewest steps up the partition tree.
     *
     * @param  schema schema of the Sleeper table
     * @param  a      values of the first key
     * @param  b      values of the second key
     * @return        the partition that includes both keys
     */
    public Partition getNearestCommonAncestor(Schema schema, Key a, Key b) {
        return getNearestCommonAncestor(getLeafPartition(schema, a), getLeafPartition(schema, b));
    }

    /**
     * Retrieve the partition whose region includes both descendent partitions. Finds the partition with the fewest
     * steps up the partition tree that is an ancestor for both.
     *
     * @param  a the first partition
     * @param  b the second partition
     * @return   the partition that is an ancestor of both
     */
    public Partition getNearestCommonAncestor(Partition a, Partition b) {
        if (a.getId().equals(b.getId())) {
            return a;
        }
        Set<String> ancestorsB = new HashSet<>(getAllAncestorIds(b.getId()));
        for (String ancestorA : getAllAncestorIds(a.getId())) {
            if (ancestorsB.contains(ancestorA)) {
                return getPartition(ancestorA);
            }
        }
        return getRootPartition();
    }

    /**
     * Traverse the partition tree visiting the leaves first. Proceeds in steps where you remove the current leaf
     * partitions and visit the new leaves.
     * <p>
     * The partitions are also ordered by the min and max of their ranges. Each time the tree is split, the partition
     * on the left/min side of the split will always be displayed first in this ordering.
     * <p>
     * This produces an ordering which is natural to read when you care the most about the leaf partitions, but you also
     * want to be able to read the rest of the tree in a predictable way.
     *
     * @return all partitions in the tree in leaves first order
     */
    public Stream<Partition> traverseLeavesFirst() {
        return traverseLeavesFirst(getLeavesInTreeOrder(), new HashSet<>(), Stream.empty());
    }

    private List<Partition> getLeavesInTreeOrder() {
        // Establish ordering by combining depth-first tree traversal with the ordering of child IDs on each partition.
        // This should ensure that partitions on the left/min side of a split will always come first in the order.
        return leavesInTreeOrderUnder(getRootPartition())
                .collect(Collectors.toList());
    }

    private Stream<Partition> leavesInTreeOrderUnder(Partition partition) {
        if (partition.isLeafPartition()) {
            return Stream.of(partition);
        }
        // Always follow left/min side first. Child partition IDs should be left/min then right/max.
        return partition.getChildPartitionIds().stream()
                .map(this::getPartition)
                .flatMap(this::leavesInTreeOrderUnder);
    }

    private Stream<Partition> traverseLeavesFirst(
            List<Partition> leaves, Set<String> prunedIds, Stream<Partition> partialTraversal) {

        // Prune the current leaves from the tree.
        // Tracking the pruned partitions creates a logical tree without needing to update the actual tree.
        leaves.stream().map(Partition::getId).forEach(prunedIds::add);

        // Find the partitions that are the new leaves of the tree after the previous ones were pruned.
        // Ensure the ordering is preserved, as the leaves were given in the correct order.
        List<Partition> nextLeaves = distinctParentsOf(leaves)
                .filter(parent -> prunedIds.containsAll(parent.getChildPartitionIds()))
                .collect(Collectors.toList());

        // Build traversal stream before recursive call, so it's tail-recursive
        Stream<Partition> traversal = Stream.concat(partialTraversal, leaves.stream());
        if (nextLeaves.isEmpty()) {
            return traversal;
        } else {
            return traverseLeavesFirst(nextLeaves, prunedIds, traversal);
        }
    }

    private Stream<Partition> distinctParentsOf(List<Partition> partitions) {
        return partitions.stream()
                .map(Partition::getParentPartitionId).filter(Objects::nonNull)
                .distinct().map(this::getPartition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionTree that = (PartitionTree) o;
        return idToPartition.equals(that.idToPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idToPartition);
    }

    @Override
    public String toString() {
        return "PartitionTree{" +
                "idToPartition=" + idToPartition +
                '}';
    }
}
