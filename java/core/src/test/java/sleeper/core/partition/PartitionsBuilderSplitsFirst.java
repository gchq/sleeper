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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A convenience class for specifying a partition tree by defining split points and then joining up the tree.
 */
public class PartitionsBuilderSplitsFirst extends PartitionsBuilder {

    protected PartitionsBuilderSplitsFirst(PartitionsBuilder builder) {
        super(builder);
    }

    private PartitionsBuilderSplitsFirst(Schema schema) {
        super(schema);
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
    public static PartitionsBuilderSplitsFirst leavesWithSplits(Schema schema, List<String> ids, List<Object> splits) {
        return leavesWithSplitsOnDimension(schema, 0, ids, splits);
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
    public static PartitionsBuilderSplitsFirst leavesWithSplitsOnDimension(Schema schema, int dimension, List<String> ids, List<Object> splits) {
        List<Region> regions = PartitionsFromSplitPoints.leafRegionsFromDimensionSplitPoints(schema, dimension, splits);
        if (ids.size() != regions.size()) {
            throw new IllegalArgumentException("Must specify IDs for all leaves before, after and in between splits");
        }
        PartitionsBuilderSplitsFirst builder = new PartitionsBuilderSplitsFirst(schema);
        for (int i = 0; i < ids.size(); i++) {
            builder.put(builder.factory.partition(ids.get(i), regions.get(i)));
        }
        return builder;
    }

    /**
     * Creates parent partitions that join the previously specified leaf partitions. This will create as many layers as
     * are required to join into a single root partition. The leaf partitions must cover the full range of the table,
     * and must be specified in order of lowest to highest values.
     *
     * @return the builder
     */
    public PartitionsBuilderSplitsFirst anyTreeJoiningAllLeaves() {
        List<Partition.Builder> mapValues = new ArrayList<>(partitionById.values());
        if (mapValues.stream().anyMatch(p -> !p.build().isLeafPartition())) {
            throw new IllegalArgumentException("Must only specify leaf partitions with no parents");
        }
        Partition.Builder left = mapValues.get(0);
        int numLeaves = partitionById.size();
        for (int i = 1; i < numLeaves; i++) {
            Partition.Builder right = mapValues.get(i);
            left = put(factory.parentJoining(UUID.randomUUID().toString(), left, right));
        }
        return this;
    }

    /**
     * Creates a parent partition that joins two previously specified partitions. The left and right partition must
     * share a common split point.
     *
     * @param  parentId unique ID for the new partition
     * @param  leftId   the ID of the partition covering the lower range of values
     * @param  rightId  the ID of the partition covering the higher range of values
     * @return          the builder
     */
    public PartitionsBuilderSplitsFirst parentJoining(String parentId, String leftId, String rightId) {
        Partition.Builder left = partitionById(leftId);
        Partition.Builder right = partitionById(rightId);
        put(factory.parentJoining(parentId, left, right));
        return this;
    }

}
