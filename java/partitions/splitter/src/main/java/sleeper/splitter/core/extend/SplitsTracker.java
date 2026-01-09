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
package sleeper.splitter.core.extend;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.splitter.core.split.SplitPartitionResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks which partitions are updated and created as splits occur.
 */
class SplitsTracker {

    private final List<Partition> updatedPartitions = new ArrayList<>();
    private final Map<String, Partition> newPartitionsById = new HashMap<>();
    private final Set<String> newLeafPartitionIds = new HashSet<>();

    SplitsTracker() {
    }

    void recordSplit(SplitPartitionResult result) {
        recordParent(result.getParentPartition());
        recordChild(result.getLeftChild());
        recordChild(result.getRightChild());
    }

    int getNumLeafPartitions() {
        return newLeafPartitionIds.size();
    }

    ExtendPartitionTreeTransaction buildTransaction() {
        return new ExtendPartitionTreeTransaction(updatedPartitions, new ArrayList<>(newPartitionsById.values()));
    }

    private void recordParent(Partition partition) {
        if (!newPartitionsById.containsKey(partition.getId())) {
            updatedPartitions.add(partition);
        } else {
            newPartitionsById.put(partition.getId(), partition);
            newLeafPartitionIds.remove(partition.getId());
        }
    }

    private void recordChild(Partition partition) {
        newPartitionsById.put(partition.getId(), partition);
        newLeafPartitionIds.add(partition.getId());
    }

}
