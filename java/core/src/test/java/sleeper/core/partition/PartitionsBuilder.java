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
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A convenience class for specifying a partition tree. This includes methods to define a tree to be readable in a test,
 * including shorthand which would not be possible with {@link PartitionFactory}.
 */
public class PartitionsBuilder {

    private final Schema schema;
    protected final PartitionFactory factory;
    protected final Map<String, Partition.Builder> partitionById;

    public PartitionsBuilder(Schema schema) {
        this.schema = schema;
        factory = new PartitionFactory(schema);
        partitionById = new LinkedHashMap<>();
    }

    protected PartitionsBuilder(PartitionsBuilder builder) {
        this.schema = builder.schema;
        this.factory = builder.factory;
        this.partitionById = builder.partitionById;
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

    protected Partition.Builder put(Partition.Builder partition) {
        partitionById.put(partition.getId(), partition);
        return partition;
    }

    protected Partition.Builder partitionById(String id) {
        return Optional.ofNullable(partitionById.get(id))
                .orElseThrow(() -> new IllegalArgumentException("Partition not specified: " + id));
    }

    /**
     * Applies a partition split to a state store, after specifying the split in this builder. You can set initial
     * partitions in this builder, initialise the state store from {@link #buildList}, define further partitions with a
     * method like {@link #splitToNewChildren}, then call this method to apply a split in the state store. This must be
     * called for each partition that was split. This must be done before splitting the new child partitions further.
     *
     * @param  stateStore          state store to update
     * @param  partitionId         the ID of the partition that was split
     * @throws StateStoreException if the state store update failed
     */
    public void applySplit(StateStore stateStore, String partitionId) throws StateStoreException {
        Partition toSplit = partitionById(partitionId).build();
        Partition left = partitionById(toSplit.getChildPartitionIds().get(0)).build();
        Partition right = partitionById(toSplit.getChildPartitionIds().get(1)).build();
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(toSplit, left, right);
    }

    /**
     * Builds the specified partitions in a list.
     *
     * @return the list of all partitions that were created
     */
    public List<Partition> buildList() {
        return partitionById.values().stream().map(Partition.Builder::build).collect(Collectors.toList());
    }

    /**
     * Builds a partition tree containing the specified partitions.
     *
     * @return a partition tree containing all partitions that were created
     */
    public PartitionTree buildTree() {
        return new PartitionTree(buildList());
    }

    public Schema getSchema() {
        return schema;
    }
}
