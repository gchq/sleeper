/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.partitions;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionsStatus {

    private final List<PartitionStatus> partitions;
    private final int numLeafPartitions;
    private final long splitThreshold;

    public PartitionsStatus(List<PartitionStatus> partitions, int numLeafPartitions, long splitThreshold) {
        this.partitions = partitions;
        this.numLeafPartitions = numLeafPartitions;
        this.splitThreshold = splitThreshold;
    }

    public static PartitionsStatus from(TableProperties tableProperties, StateStore store) throws StateStoreException {
        List<Partition> partitions = store.getAllPartitions();
        long splitThreshold = tableProperties.getLong(PARTITION_SPLIT_THRESHOLD);
        if (partitions.isEmpty()) {
            return new PartitionsStatus(Collections.emptyList(), 0, splitThreshold);
        }
        List<PartitionStatus> statuses = statusesFrom(tableProperties, partitions, store.getActiveFiles());
        int numLeafPartitions = (int) partitions.stream().filter(Partition::isLeafPartition).count();
        return new PartitionsStatus(statuses, numLeafPartitions, splitThreshold);
    }

    private static List<PartitionStatus> statusesFrom(
            TableProperties tableProperties, List<Partition> partitions, List<FileInfo> activeFiles) {

        PartitionTree tree = new PartitionTree(tableProperties.getSchema(), partitions);
        return traverseLeavesFirst(tree)
                .map(partition -> PartitionStatus.from(tableProperties, tree, partition, activeFiles))
                .collect(Collectors.toList());
    }

    private static Stream<Partition> traverseLeavesFirst(PartitionTree tree) {
        List<Partition> leaves = getLeavesInTreeOrder(tree);
        return traverseLeavesFirst(tree, leaves, new HashSet<>());
    }

    private static List<Partition> getLeavesInTreeOrder(PartitionTree tree) {
        return leavesInTreeOrderUnder(tree.getRootPartition(), tree).collect(Collectors.toList());
    }

    private static Stream<Partition> leavesInTreeOrderUnder(Partition partition, PartitionTree tree) {
        if (partition.isLeafPartition()) {
            return Stream.of(partition);
        }
        return partition.getChildPartitionIds().stream()
                .map(tree::getPartition)
                .flatMap(child -> leavesInTreeOrderUnder(child, tree));
    }

    private static Stream<Partition> traverseLeavesFirst(
            PartitionTree tree, List<Partition> leaves, Set<String> prunedIds) {

        if (leaves.isEmpty()) {
            return Stream.empty();
        }

        // Prune the current leaves from the tree.
        // Tracking the pruned partitions creates a logical tree without needing to update the actual tree.
        leaves.stream().map(Partition::getId).forEach(prunedIds::add);

        // Find the partitions that are the new leaves of the tree after the previous ones were pruned.
        // Ensure the ordering is preserved, as the leaves were given in the correct order.
        List<Partition> nextLeaves = leaves.stream()
                .map(Partition::getParentPartitionId).filter(Objects::nonNull)
                .distinct().map(tree::getPartition)
                .filter(parent -> prunedIds.containsAll(parent.getChildPartitionIds()))
                .collect(Collectors.toList());

        return Stream.concat(leaves.stream(),
                traverseLeavesFirst(tree, nextLeaves, prunedIds));
    }

    public int getNumPartitions() {
        return partitions.size();
    }

    public int getNumLeafPartitions() {
        return numLeafPartitions;
    }

    public long getNumSplittingPartitions() {
        return partitions.stream().filter(PartitionStatus::isNeedsSplitting).count();
    }

    public long getSplitThreshold() {
        return splitThreshold;
    }

    public List<PartitionStatus> getPartitions() {
        return partitions;
    }

}
