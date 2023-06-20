/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.status.report.partitions;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

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
        List<PartitionStatus> statuses = statusesFrom(tableProperties, partitions, store.getFileInPartitionList());
        return new PartitionsStatus(statuses, splitThreshold);
    }

    private static List<PartitionStatus> statusesFrom(
            TableProperties tableProperties, List<Partition> partitions, List<FileInfo> activeFiles) {

        PartitionTree tree = new PartitionTree(tableProperties.getSchema(), partitions);
        return tree.traverseLeavesFirst()
                .map(partition -> PartitionStatus.from(tableProperties, tree, partition, activeFiles))
                .collect(Collectors.toList());
    }

    public int getNumPartitions() {
        return partitions.size();
    }

    public long getNumLeafPartitions() {
        return partitions.stream().filter(PartitionStatus::isLeafPartition).count();
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
