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
package sleeper.core.deploy;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.table.TableProperties;

import java.util.List;

/**
 * Configuration to deploy a Sleeper table.
 *
 * @param properties        the table properties
 * @param initialPartitions the partitions to initialise the table when it is first created or reinitialised
 */
public record SleeperTableConfiguration(TableProperties properties, List<Partition> initialPartitions) {

    /**
     * Validates the configuration.
     *
     * @throws SleeperPropertiesInvalidException if any property is invalid
     * @throws IllegalArgumentException          on a validation failure
     */
    public void validate() {
        properties.validate();
        PartitionTree tree = new PartitionTree(initialPartitions);
        List<String> unlinkedPartitionIds = initialPartitions.stream()
                .filter(partition -> !isPartitionLinkedToRoot(tree, partition))
                .map(Partition::getId)
                .toList();
        if (!unlinkedPartitionIds.isEmpty()) {
            throw new IllegalArgumentException("Found partitions unlinked to the rest of the tree: " + unlinkedPartitionIds);
        }
    }

    private boolean isPartitionLinkedToRoot(PartitionTree tree, Partition partition) {
        if (partition.getParentPartitionId() == null) {
            return true;
        }
        Partition parent = tree.getPartition(partition.getParentPartitionId());
        if (parent == null) {
            return false;
        }
        if (!parent.getChildPartitionIds().contains(partition.getId())) {
            return false;
        }
        return isPartitionLinkedToRoot(tree, parent);
    }

}
