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
package sleeper.core.partition;

import sleeper.core.key.Key;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Represents a tree of partitions. It can be used to traverse or query the tree, e.g. to find all ancestors of a
 * partition, partitions that are either parents of the partition, or grandparents, or great-grandparents.
 */
public class PartitionTree {
    private final Map<String, Partition> idToPartition;
    private final Partition rootPartition;

    public PartitionTree(Collection<Partition> partitions) {
        this.idToPartition = new TreeMap<>();
        partitions.forEach(p -> this.idToPartition.put(p.getId(), p));
        List<Partition> rootPartitions = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList());
        // There should be exactly one root partition.
        if (rootPartitions.size() != 1) {
            throw new IllegalArgumentException("There should be exactly one root partition, found " + rootPartitions.size());
        }
        this.rootPartition = rootPartitions.get(0);
    }

    /**
     * Validates that the partition tree is correctly linked together.
     *
     * @param schema the Sleeper table schema
     */
    public void validate(Schema schema) {
        List<String> unlinkedPartitionIds = idToPartition.values().stream()
                .filter(partition -> !isPartitionLinkedToRoot(partition))
                .map(Partition::getId)
                .toList();
        if (!unlinkedPartitionIds.isEmpty()) {
            throw new IllegalArgumentException("Found partitions unlinked to the rest of the tree: " + unlinkedPartitionIds);
        }
        List<String> missingChildPartitionIds = new ArrayList<>();
        findMissingChildPartitions(rootPartition, missingChildPartitionIds);
        if (!missingChildPartitionIds.isEmpty()) {
            throw new IllegalArgumentException("Found missing child partitions: " + missingChildPartitionIds);
        }
        findInvalidSplits(schema, rootPartition);
    }

    /**
     * Validates that a partition split covers the range of the parent partition.
     *
     * @param parent   the parent partition
     * @param children the child partitions split from the parent
     * @param schema   the Sleeper table schema
     */
    public static void validateSplit(Partition parent, List<Partition> children, Schema schema) {
        if (children.size() != 2) {
            throw new IllegalArgumentException("Expected 2 child partitions under " + parent.getId() + ", left and right of split point, found " + children.size());
        }
        if (schema.getRowKeyFields().size() <= parent.getDimension()) {
            throw new IllegalArgumentException("No row key at dimension " + parent.getDimension() + " for parent partition " + parent.getId());
        }
        Field splitField = schema.getRowKeyFields().get(parent.getDimension());
        Partition child1 = children.get(0);
        Partition child2 = children.get(1);
        Range range1 = child1.getRegion().getRange(splitField.getName());
        Range range2 = child2.getRegion().getRange(splitField.getName());
        PrimitiveType type = (PrimitiveType) splitField.getType();
        Partition leftChild;
        Partition rightChild;
        if (type.compare(range1.getMax(), range2.getMin()) == 0) {
            leftChild = child1;
            rightChild = child2;
        } else if (type.compare(range1.getMin(), range2.getMax()) == 0) {
            leftChild = child2;
            rightChild = child1;
        } else {
            throw new IllegalArgumentException("Child partitions do not meet at a split point on dimension " + parent.getDimension() + " set in parent partition " + parent.getId());
        }
        List<String> expectedChildIds = List.of(leftChild.getId(), rightChild.getId());
        if (!expectedChildIds.equals(parent.getChildPartitionIds())) {
            throw new IllegalArgumentException("Child partition IDs do not match expected order in parent partition " + parent.getId() + ", expected: " + expectedChildIds);
        }
        Range parentRange = parent.getRegion().getRange(splitField.getName());
        Range leftRange = leftChild.getRegion().getRange(splitField.getName());
        Range rightRange = rightChild.getRegion().getRange(splitField.getName());
        if (type.compare(parentRange.getMin(), leftRange.getMin()) != 0) {
            throw new IllegalArgumentException("Left child partition " + leftChild.getId() + " does not match boundary of parent " + parent.getId());
        }
        if (type.compare(parentRange.getMax(), rightRange.getMax()) != 0) {
            throw new IllegalArgumentException("Right child partition " + rightChild.getId() + " does not match boundary of parent " + parent.getId());
        }
    }

    /**
     * Retrieves IDs of partitions that were split from a given parent partition.
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
     * Retrieves IDs of all partitions that were split to result in the given partition. Starts with the most recent
     * parent, and includes all ancestors of that parent ending with the root partition.
     *
     * @param  partitionId the ID of the partition to find ancestors of
     * @return             all IDs of the partition's ancestors
     */
    public List<String> getAllAncestorIds(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }
        return ancestorsOf(idToPartition.get(partitionId)).map(Partition::getId).collect(toList());
    }

    /**
     * Retrieves all partitions that were split to result in the given partition. Starts with the most recent parent,
     * and includes all ancestors of that parent ending with the root partition.
     *
     * @param  partitionId the ID of the partition to find ancestors of
     * @return             all of the partition's ancestors
     */
    public List<Partition> getAllAncestors(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }
        return ancestorsOf(idToPartition.get(partitionId)).collect(toList());
    }

    /**
     * Streams through all partitions that were split to result in the given partition. Starts with the most recent
     * parent, and includes all ancestors of that parent ending with the root partition.
     *
     * @param  partition the ID of the partition to find ancestors of
     * @return           all of the partition's ancestors
     */
    public Stream<Partition> ancestorsOf(Partition partition) {
        return Stream.iterate(getParent(partition),
                currentPartition -> currentPartition != null,
                currentPartition -> getParent(currentPartition));
    }

    /**
     * Streams through all partitions that are descendents of the given partition. Starts with the child partitions,
     * then finds their child partitions, and so on. Traverses depth first.
     *
     * @param  partition the partition
     * @return           the partition's descendents
     */
    public Stream<Partition> descendentsOf(Partition partition) {
        return partition.getChildPartitionIds()
                .stream().map(this::getPartition)
                .flatMap(child -> Stream.concat(Stream.of(child), descendentsOf(child)));
    }

    /**
     * Retrieves the parent of a partition.
     *
     * @param  partition the partition
     * @return           its parent, or null if it has no parent
     */
    public Partition getParent(Partition partition) {
        if (partition.getParentPartitionId() == null) {
            return null;
        }
        return idToPartition.get(partition.getParentPartitionId());
    }

    /**
     * Retrieves a partition by its unique ID.
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
     * Streams through all partitions.
     *
     * @return the stream
     */
    public Stream<Partition> streamPartitions() {
        return idToPartition.values().stream();
    }

    /**
     * Streams through all leaf partitions. This is all partitions that have no child partitions, and no split point.
     * Other partitions make up the root and branches of the partition tree, where the space of the values of key
     * fields is split into a number of child partitions.
     *
     * @return the stream
     */
    public Stream<Partition> streamLeafPartitions() {
        return streamPartitions().filter(Partition::isLeafPartition);
    }

    public List<Partition> getLeafPartitions() {
        return streamLeafPartitions().toList();
    }

    /**
     * Retrieves the leaf partition that includes the given key.
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
     * Retrieves the partition whose region includes both keys. Finds the nearest partition to each of the keys, with
     * the fewest steps up the partition tree.
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
     * Retrieves the partition whose region includes both descendent partitions. Finds the partition with the fewest
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
     * Traverses the partition tree visiting the leaves first. Proceeds in steps where you remove the current leaf
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
        return traverseLeavesFirst(streamLeavesInTreeOrder().toList(), new HashSet<>(), Stream.empty());
    }

    /**
     * Traverses all leaf partitions by visiting every node in the partition tree. Starts at the root and goes left
     * first then right.
     *
     * @return all leaf partitions in order of their position in the tree
     */
    public Stream<Partition> streamLeavesInTreeOrder() {
        // Establish ordering by combining depth-first tree traversal with the ordering of child IDs on each partition.
        // This should ensure that partitions on the left/min side of a split will always come first in the order.
        return leavesInTreeOrderUnder(getRootPartition());
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

    private boolean isPartitionLinkedToRoot(Partition partition) {
        if (partition.getParentPartitionId() == null) {
            return true;
        }
        Partition parent = getPartition(partition.getParentPartitionId());
        if (parent == null) {
            return false;
        }
        if (!parent.getChildPartitionIds().contains(partition.getId())) {
            return false;
        }
        return isPartitionLinkedToRoot(parent);
    }

    private void findMissingChildPartitions(Partition partition, List<String> missingPartitionIds) {
        for (String childId : partition.getChildPartitionIds()) {
            Partition child = getPartition(childId);
            if (child == null) {
                missingPartitionIds.add(childId);
            } else {
                findMissingChildPartitions(child, missingPartitionIds);
            }
        }
    }

    private void findInvalidSplits(Schema schema, Partition partition) {
        if (partition.isLeafPartition()) {
            if (!partition.getChildPartitionIds().isEmpty()) {
                throw new IllegalArgumentException("Partition has " + partition.getChildPartitionIds().size() + " child partitions but is marked as a leaf partition: " + partition.getId());
            }
            return;
        }
        List<Partition> childPartitions = partition.getChildPartitionIds().stream()
                .map(this::getPartition)
                .toList();
        validateSplit(partition, childPartitions, schema);
        for (Partition child : childPartitions) {
            findInvalidSplits(schema, child);
        }
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
