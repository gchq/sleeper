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
import java.util.LinkedHashMap;
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
    public static PartitionTree createSubtree(PartitionTree originalTree, int leafPartitionCount) throws PartitionTreeException {
        if (leafPartitionCount > originalTree.getLeafPartitions().size()) {
            throw new PartitionTreeException("Requested size of " + leafPartitionCount + " is greater than input tree capacity", originalTree);
        }

        LinkedHashMap<String, Partition> partitionsToBuildSubTree = new LinkedHashMap<String, Partition>();
        LinkedHashMap<String, Partition> nextLevelToEvaluate = new LinkedHashMap<String, Partition>();

        partitionsToBuildSubTree.put(originalTree.getRootPartition().getId(), originalTree.getRootPartition());
        nextLevelToEvaluate.put(originalTree.getRootPartition().getId(), originalTree.getRootPartition());
        int presentLeafCount = 1;

        while (checkIfLeafCountMet(presentLeafCount, leafPartitionCount)) {
            Iterator<String> nextNodeIterator = getChildPartitionsFromIds(originalTree, nextLevelToEvaluate.keySet()).iterator();
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

    private static List<String> getChildPartitionsFromIds(PartitionTree treeIn, Set<String> parentIdsIn) {
        List<String> outList = new ArrayList<String>();
        parentIdsIn.forEach(parentId -> {
            treeIn.getChildIds(parentId).forEach(childId -> {
                outList.add(childId);
            });
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
