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
import java.util.Objects;

/**
 * Generates a subtree of PartitionTree for given number of leaf nodes.
 */
public class PartitionSubTree extends PartitionTree {
    private final ArrayList<String> resetLeafIds = new ArrayList<>();

    public PartitionSubTree(PartitionTree originalTree, int leafPartitionCount) {
        super(List.of(originalTree.getRootPartition()));

        // Check loop has count incremented by 1 to account for root partition that always must exist
        while (idToPartition.values().size() < leafPartitionCount + 1) {
            resetLeafIds.clear();
            Collection<Partition> presentBatch = List.copyOf(idToPartition.values());
            presentBatch.forEach(partition -> {
                partition.getChildPartitionIds().forEach(
                        partitionId -> {
                            if (!idToPartition.containsKey(partitionId)) {
                                idToPartition.put(partitionId, originalTree.getPartition(partitionId));
                                resetLeafIds.add(partitionId);
                            }
                        });
            });
        }

        // Last gathered selection of ids are set to be leaves of new tree.
        resetLeafIds.forEach(leafId -> {
            idToPartition.put(leafId, idToPartition.get(leafId)
                    .toBuilder()
                    .leafPartition(true)
                    .build());
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionSubTree that = (PartitionSubTree) o;
        return idToPartition.equals(that.idToPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idToPartition);
    }
}
