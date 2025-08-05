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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class to generate subtree from given leaf partition requirements.
 */
public class PartitionSubTreeFactory {
    //private static final

    private PartitionSubTreeFactory() {
    }

    /**
     * Generates a subTree from a given tree with a target leaf count. Actual result may exceed the count requested
     * as presently will go to a depth until the count is matched or exceeded.
     *
     * @param  originalTree       source from which the new sub tree is to be created
     * @param  leafPartitionCount amount of leaves to be contained in the new tree at a minimum
     * @return                    newly generated sub tree
     */
    public static PartitionTree createSubTree(PartitionTree originalTree, int leafPartitionCount) throws RuntimeException {
        if (leafPartitionCount > originalTree.getAllPartitions().size()) {
            throw new RuntimeException();
        }
        ArrayList<String> resetLeafIds = new ArrayList<>();
        PartitionTree subTree = new PartitionTree(List.of(originalTree.getRootPartition()));
        resetLeafIds.add(originalTree.getRootPartition().getId());

        // Check loop has count incremented by 1 to account for root partition that always must exist
        while (subTree.idToPartition.values().size() < leafPartitionCount + 1) {
            resetLeafIds.clear();
            Collection<Partition> presentBatch = List.copyOf(subTree.idToPartition.values());
            presentBatch.forEach(partition -> {
                partition.getChildPartitionIds().forEach(
                        partitionId -> {
                            if (!subTree.idToPartition.containsKey(partitionId)) {
                                subTree.idToPartition.put(partitionId, originalTree.getPartition(partitionId));
                                resetLeafIds.add(partitionId);
                            }
                        });
            });
        }

        // Last gathered selection of ids have their key details reset to act as leaves of new tree.
        resetLeafIds.forEach(leafId -> {
            subTree.idToPartition.put(leafId, adjustToLeafStatus(subTree.idToPartition.get(leafId)));

        });

        // If root is the only partition, return a new sub tree with root corrected for no leaves
        if (subTree.getAllPartitions().size() == 1) {
            return new PartitionTree(subTree.getAllPartitions());
        }

        return subTree;
    }

    private static Partition adjustToLeafStatus(Partition partitionIn) {
        return partitionIn.toBuilder()
                .leafPartition(true)
                .childPartitionIds(List.of())
                .dimension(-1)
                .build();
    }
}
