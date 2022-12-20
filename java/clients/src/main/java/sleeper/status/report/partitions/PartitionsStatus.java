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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
        List<Partition> leaves = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        List<PartitionStatus> statuses = new ArrayList<>();
        forEachLeavesFirst(tree, leaves, partition -> statuses.add(
                PartitionStatus.from(tableProperties, tree, partition, activeFiles)));
        return statuses;
    }

    private static void forEachLeavesFirst(PartitionTree tree, List<Partition> leaves, Consumer<Partition> operation) {
        Set<String> ids = leaves.stream().map(Partition::getId).collect(Collectors.toSet());
        forEachLeavesFirst(tree, leaves, ids, operation);
    }

    private static void forEachLeavesFirst(
            PartitionTree tree, List<Partition> leaves, Set<String> visitedIds, Consumer<Partition> operation) {

        if (leaves.isEmpty()) {
            return;
        }
        leaves.sort(Comparator.comparing(partition -> getOrderByString(partition, tree)));
        leaves.forEach(operation);

        Set<String> nextIds = leaves.stream()
                .map(Partition::getParentPartitionId)
                .filter(parentId -> isNextLeaf(parentId, tree, visitedIds))
                .collect(Collectors.toSet());
        visitedIds.addAll(nextIds);
        List<Partition> nextLeaves = nextIds.stream().map(tree::getPartition).collect(Collectors.toList());
        forEachLeavesFirst(tree, nextLeaves, visitedIds, operation);
    }

    private static String getOrderByString(Partition partition, PartitionTree tree) {
        String parentId = partition.getParentPartitionId();
        if (parentId == null) {
            return partition.getId();
        }
        Partition parent = tree.getPartition(parentId);
        int childIndex = parent.getChildPartitionIds().indexOf(partition.getId());
        return parentId + "." + childIndex + "." + partition.getId();
    }

    private static boolean isNextLeaf(String partitionId, PartitionTree tree, Set<String> visitedIds) {
        if (partitionId == null) {
            return false;
        }
        Partition partition = tree.getPartition(partitionId);
        return visitedIds.containsAll(partition.getChildPartitionIds());
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
