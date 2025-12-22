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
package sleeper.bulkimport.runner.common;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;

import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.stream.Collectors.toCollection;

/**
 * Assigns integers to the leaf nodes of a given partition tree. This is used when partitioning data according to the
 * Sleeper partition tree, since Spark expects an integer partition identifier. When a partition tree is broadcast
 * across a Spark cluster, we want each leaf partition to have a reproducible integer that will be the same everywhere.
 */
public class PartitionNumbers {

    private PartitionNumbers() {
    }

    /**
     * Creates a mapping from Sleeper partition ID to an integer.
     *
     * @param  tree the partition tree
     * @return      the mapping
     */
    public static Map<String, Integer> getPartitionIdToInt(PartitionTree tree) {
        // Sort the leaf partitions by id so that we can create a mapping from partition id to
        // int in a way that is consistent across multiple calls to this function across different
        // executors in the same Spark job.
        Map<String, Integer> partitionIdToInt = new TreeMap<>();
        int i = 0;
        for (String leafPartitionId : getSortedLeafPartitionIds(tree)) {
            partitionIdToInt.put(leafPartitionId, i);
            i++;
        }
        return partitionIdToInt;
    }

    private static TreeSet<String> getSortedLeafPartitionIds(PartitionTree tree) {
        return tree.streamLeafPartitions()
                .map(Partition::getId)
                .collect(toCollection(TreeSet::new));
    }

}
