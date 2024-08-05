/*
 * Copyright 2022-2024 Crown Copyright
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

/**
 * A convenience class for specifying a partition tree by starting from the root and splitting down the tree.
 */
public class PartitionsBuilderRootFirst extends PartitionsBuilder {

    protected PartitionsBuilderRootFirst(PartitionsBuilder builder) {
        super(builder);
    }

    /**
     * Creates new partitions by splitting a previously defined partition.
     *
     * @param  parentId   the ID of the partition to split
     * @param  leftId     unique ID for the new partition covering values lower than the split point
     * @param  rightId    unique ID for the new partition covering values equal to or higher than the split point
     * @param  splitPoint value for the first row key to split on
     * @return            the builder
     */
    public PartitionsBuilderRootFirst splitToNewChildren(
            String parentId, String leftId, String rightId, Object splitPoint) {
        return splitToNewChildrenOnDimension(parentId, leftId, rightId, 0, splitPoint);
    }

    /**
     * Creates new partitions by splitting a previously defined partition on a particular row key.
     *
     * @param  parentId   the ID of the partition to split
     * @param  leftId     unique ID for the new partition covering values lower than the split point
     * @param  rightId    unique ID for the new partition covering values equal to or higher than the split point
     * @param  dimension  index of the row key to split on
     * @param  splitPoint value for the row key to split on
     * @return            the builder
     */
    public PartitionsBuilderRootFirst splitToNewChildrenOnDimension(
            String parentId, String leftId, String rightId, int dimension, Object splitPoint) {
        Partition.Builder parent = partitionById(parentId);
        PartitionSplitResult splitResult = factory.split(parent.build(), leftId, rightId, dimension, splitPoint);
        splitResult.getChildren().forEach(this::put);
        put(splitResult.getParent());
        return this;
    }
}
