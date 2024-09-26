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
package sleeper.splitter.status;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionsStatus {

    private final List<PartitionStatus> partitions;
    private final long splitThreshold;

    public PartitionsStatus(List<PartitionStatus> partitions, long splitThreshold) {
        this.partitions = partitions;
        this.splitThreshold = splitThreshold;
    }

    public static PartitionsStatus from(TableProperties tableProperties, StateStore store) throws StateStoreException {
        List<Partition> partitions = store.getAllPartitions();
        long splitThreshold = tableProperties.getLong(PARTITION_SPLIT_THRESHOLD);
        if (partitions.isEmpty()) {
            return new PartitionsStatus(Collections.emptyList(), splitThreshold);
        }
        List<PartitionStatus> statuses = statusesFrom(tableProperties, partitions, store.getFileReferences());
        return new PartitionsStatus(statuses, splitThreshold);
    }

    private static List<PartitionStatus> statusesFrom(
            TableProperties tableProperties, List<Partition> partitions, List<FileReference> activeFiles) {

        PartitionTree tree = new PartitionTree(partitions);
        Map<String, List<FileReference>> fileReferencesByPartition = activeFiles.stream()
                .collect(groupingBy(FileReference::getPartitionId));
        return tree.traverseLeavesFirst()
                .map(partition -> PartitionStatus.from(tableProperties, tree, partition, fileReferencesByPartition))
                .collect(Collectors.toList());
    }

    public int getNumPartitions() {
        return partitions.size();
    }

    public long getNumLeafPartitions() {
        return partitions.stream().filter(PartitionStatus::isLeafPartition).count();
    }

    public long getNumLeafPartitionsThatWillBeSplit() {
        return partitions.stream()
                .filter(PartitionStatus::isLeafPartition)
                .filter(PartitionStatus::willBeSplit)
                .count();
    }

    public long getSplitThreshold() {
        return splitThreshold;
    }

    public List<PartitionStatus> getPartitions() {
        return partitions;
    }

}
