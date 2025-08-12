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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class to generate subtree from given leaf partition requirements.
 */
public class PartitionSubtreeFactory {

    private PartitionSubtreeFactory() {
    }

    /**
     * Generates a subtree from a given tree with a target leaf count.
     *
     * @param  originalTree       source from which the new sub tree is to be created
     * @param  leafPartitionCount amount of leaves to be contained in the new tree at a minimum
     * @param  bias               determines when creating a subtree which side of nodes to focus on first
     * @return                    newly generated sub tree
     */
    public static PartitionTree createSubtree(PartitionTree originalTree, int leafPartitionCount, PartitionTreeBias bias) throws PartitionTreeException {
        if (leafPartitionCount > originalTree.getLeafPartitions().size()) {
            throw new PartitionTreeException("Requested size of " + leafPartitionCount + " is greater than input tree capacity");
        }

        PartitionTree subtree = new PartitionTree(List.of(originalTree.getRootPartition()));
        List<Partition> lastAddedIds = new ArrayList<Partition>();
        lastAddedIds.add(originalTree.getRootPartition());
        int presentLeafCount = 1;

        while (checkIfLeafCountMet(presentLeafCount, leafPartitionCount)) {
            Iterator<Partition> partitionIterator = getChildPartitionsFromIds(originalTree, lastAddedIds, bias).iterator();
            while (partitionIterator.hasNext() && checkIfLeafCountMet(presentLeafCount, leafPartitionCount)) {
                Partition presentPartion = partitionIterator.next();
                subtree.idToPartition.put(presentPartion.getId(), presentPartion);
                presentLeafCount++;

                if (lastAddedIds.contains(subtree.getPartition(presentPartion.getParentPartitionId()))) {
                    presentLeafCount--;
                    lastAddedIds.remove(subtree.getPartition(presentPartion.getParentPartitionId()));
                }
                if (!presentPartion.isLeafPartition()) {
                    lastAddedIds.add(presentPartion);
                }
            }
        }

        lastAddedIds.forEach(partition -> {
            if (!partition.isLeafPartition()) {
                subtree.idToPartition.put(partition.getId(), adjustToLeafStatus(partition));
            }
        });

        return subtree;
    }

    private static boolean checkIfLeafCountMet(int present, int target) {
        return present < target;
    }

    private static List<Partition> getChildPartitionsFromIds(PartitionTree treeIn, List<Partition> parentIdsIn, PartitionTreeBias bias) {
        List<Partition> outList = new ArrayList<Partition>();
        parentIdsIn.forEach(partition -> {
            partition.getChildPartitionIds().forEach(childId -> {
                outList.add(treeIn.getPartition(childId));
            });
        });
        if (bias.equals(PartitionTreeBias.LEFT_BIAS)) {
            outList.sort(new PartitionComparator());
        } else if (bias.equals(PartitionTreeBias.RIGHT_BIAS)) {
            outList.sort(new PartitionComparator().reversed());
        }

        return outList;
    }

    private static Partition adjustToLeafStatus(Partition partitionIn) {
        return partitionIn.toBuilder()
                .leafPartition(true)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
    }

}
