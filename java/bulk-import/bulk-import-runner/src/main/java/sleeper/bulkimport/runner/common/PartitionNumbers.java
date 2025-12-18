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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.stream.Collectors.toCollection;

public class PartitionNumbers {

    private PartitionNumbers() {
    }

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

    public static List<Partition> getIntToPartition(PartitionTree tree) {
        return getSortedLeafPartitionIds(tree).stream()
                .map(tree::getPartition)
                .toList();
    }

    private static TreeSet<String> getSortedLeafPartitionIds(PartitionTree tree) {
        return tree.streamLeafPartitions()
                .map(Partition::getId)
                .collect(toCollection(TreeSet::new));
    }

}
