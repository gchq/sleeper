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

import sleeper.core.range.Region;
import sleeper.core.schema.Schema;

import java.util.List;

/**
 * A convenience class for specifying a partition tree. This includes methods to define a tree to be readable in a test,
 * including shorthand which would not be possible with {@link PartitionFactory}.
 */
public class PartitionsBuilder extends PartitionsBuilderBase {

    public PartitionsBuilder(Schema schema) {
        super(schema);
    }

    /**
     * Creates a root partition that can be further split into child partitions. This can be used to build a partition
     * tree from the root down.
     *
     * @param  rootId unique ID for the new root partition
     * @return        the builder
     * @see           PartitionsBuilderRootFirst#splitToNewChildren
     * @see           PartitionsBuilderRootFirst#splitToNewChildrenOnDimension
     */
    public PartitionsBuilderRootFirst rootFirst(String rootId) {
        put(factory.rootFirst(rootId));
        return new PartitionsBuilderRootFirst(this);
    }

    /**
     * Creates a tree with just one partition. This will be a root partition and the only leaf partition.
     *
     * @param  id unique ID for the partition
     * @return    the builder
     */
    public PartitionsBuilderRootFirst singlePartition(String id) {
        return rootFirst(id);
    }

    /**
     * Creates partially constructed leaf partitions. Parent partitions must be defined separately that join the
     * partitions together into a tree.
     *
     * @param  ids    unique IDs for the leaves
     * @param  splits values of the first row key, for split points in between the new leaf partitions
     * @return        the builder
     * @see           PartitionsBuilderSplitsFirst#anyTreeJoiningAllLeaves()
     * @see           PartitionsBuilderSplitsFirst#parentJoining
     */
    public PartitionsBuilderSplitsFirst leavesWithSplits(List<String> ids, List<Object> splits) {
        return leavesWithSplitsOnDimension(0, ids, splits);
    }

    /**
     * Creates partially constructed leaf partitions split on a certain row key. Parent partitions must be defined
     * separately that join the partitions together into a tree.
     *
     * @param  dimension index in the schema of the row key the partitions are split on
     * @param  ids       unique IDs for the leaves
     * @param  splits    values of the row key at the specified dimension, for split points in between the new leaf
     *                   partitions
     * @return           the builder
     * @see              PartitionsBuilderSplitsFirst#anyTreeJoiningAllLeaves()
     * @see              PartitionsBuilderSplitsFirst#parentJoining
     */
    public PartitionsBuilderSplitsFirst leavesWithSplitsOnDimension(int dimension, List<String> ids, List<Object> splits) {
        List<Region> regions = PartitionsFromSplitPoints.leafRegionsFromDimensionSplitPoints(schema, dimension, splits);
        if (ids.size() != regions.size()) {
            throw new IllegalArgumentException("Must specify IDs for all leaves before, after and in between splits");
        }
        for (int i = 0; i < ids.size(); i++) {
            put(factory.partition(ids.get(i), regions.get(i)));
        }
        return new PartitionsBuilderSplitsFirst(this);
    }
}
