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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
     * @return                    newly generated sub tree
     */
    public static PartitionTree createSubtree(PartitionTree originalTree, int leafPartitionCount, PartitionTreeBias bias) throws PartitionTreeException {
        if (leafPartitionCount > originalTree.getLeafPartitions().size()) {
            throw new PartitionTreeException("Requested size of " + leafPartitionCount + " is greater than input tree capacity");
        }

        HashMap<String, Partition> partitionsToBuildSubTree = new HashMap<String, Partition>();
        HashMap<String, Partition> nextLevelToEvaluate = new HashMap<String, Partition>();

        partitionsToBuildSubTree.put(originalTree.getRootPartition().getId(), originalTree.getRootPartition());
        nextLevelToEvaluate.put(originalTree.getRootPartition().getId(), originalTree.getRootPartition());
        int presentLeafCount = 1;

        while (checkIfLeafCountMet(presentLeafCount, leafPartitionCount)) {
            Iterator<String> nextNodeIterator = getChildPartitionsFromIds(originalTree, nextLevelToEvaluate.keySet(), bias).iterator();
            while (nextNodeIterator.hasNext() && checkIfLeafCountMet(presentLeafCount, leafPartitionCount)) {
                Partition nextPartition = originalTree.getPartition(nextNodeIterator.next());
                partitionsToBuildSubTree.put(nextPartition.getId(), nextPartition);

                if (nextLevelToEvaluate.containsKey(nextPartition.getParentPartitionId())) {
                    nextLevelToEvaluate.remove(nextPartition.getParentPartitionId());
                } else {
                    presentLeafCount++;
                }

                nextLevelToEvaluate.put(nextPartition.getId(), nextPartition);
            }
        }
        nextLevelToEvaluate.keySet().forEach(remainingNodeId -> {
            if (!nextLevelToEvaluate.get(remainingNodeId).isLeafPartition()) {
                partitionsToBuildSubTree.put(remainingNodeId, adjustToLeafStatus(partitionsToBuildSubTree.get(remainingNodeId)));
            }
        });

        return new PartitionTree(partitionsToBuildSubTree.values());
    }

    private static boolean checkIfLeafCountMet(int present, int target) {
        return present < target;
    }

    private static List<String> getChildPartitionsFromIds(PartitionTree treeIn, Set<String> parentIdsIn, PartitionTreeBias bias) {
        List<Partition> sortList = new ArrayList<Partition>();
        parentIdsIn.forEach(parentId -> {
            treeIn.getChildPartitions(parentId).forEach(childId -> {
                sortList.add(childId);
            });
        });
        if (bias.equals(PartitionTreeBias.LEFT_BIAS)) {
            sortList.sort(new PartitionComparator());
        } else if (bias.equals(PartitionTreeBias.RIGHT_BIAS)) {
            sortList.sort(new PartitionComparator().reversed());
        }

        List<String> outList = new ArrayList<String>();
        sortList.forEach(partition -> {
            outList.add(partition.getId());
        });
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
