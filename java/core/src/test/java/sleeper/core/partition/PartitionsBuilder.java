/*
 * Copyright 2022-2023 Crown Copyright
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A convenience class for specifying partitions.
 * <p>
 * Note that a shorthand is used for cases where we have a schema with only one row key field.
 * This will not be useful in the general case.
 */
public class PartitionsBuilder {

    private final Schema schema;
    private final PartitionFactory factory;
    private final List<Partition.Builder> partitions = new ArrayList<>();
    private final Map<String, Partition.Builder> partitionById = new HashMap<>();

    public PartitionsBuilder(Schema schema) {
        this.schema = schema;
        factory = new PartitionFactory(schema);
    }

    public PartitionsBuilder singlePartition(String id) {
        return leavesWithSplits(Collections.singletonList(id), Collections.emptyList());
    }

    public PartitionsBuilder leavesWithSplits(List<String> ids, List<Object> splits) {
        return leavesWithSplitsOnDimension(0, ids, splits);
    }

    public PartitionsBuilder leavesWithSplitsOnDimension(int dimension, List<String> ids, List<Object> splits) {
        List<Region> regions = PartitionsFromSplitPoints.leafRegionsFromDimensionSplitPoints(schema, dimension, splits);
        if (ids.size() != regions.size()) {
            throw new IllegalArgumentException("Must specify IDs for all leaves before, after and in between splits");
        }
        for (int i = 0; i < ids.size(); i++) {
            add(factory.partition(ids.get(i), regions.get(i)));
        }
        return this;
    }

    public PartitionsBuilder anyTreeJoiningAllLeaves() {
        if (partitions.stream().anyMatch(p -> !p.build().isLeafPartition())) {
            throw new IllegalArgumentException("Must only specify leaf partitions with no parents");
        }
        Partition.Builder left = partitions.get(0);
        int numLeaves = partitions.size();
        for (int i = 1; i < numLeaves; i++) {
            Partition.Builder right = partitions.get(i);
            left = add(factory.parentJoining(UUID.randomUUID().toString(), left, right));
        }
        return this;
    }

    public PartitionsBuilder parentJoining(String parentId, String leftId, String rightId) {
        Partition.Builder left = partitionById(leftId);
        Partition.Builder right = partitionById(rightId);
        add(factory.parentJoining(parentId, left, right));
        return this;
    }

    public PartitionsBuilder rootFirst(String rootId) {
        add(factory.rootFirst(rootId));
        return this;
    }

    public PartitionsBuilder treeWithSingleSplitPoint(Object splitPoint) {
        return fromRoot(root -> root.split(splitPoint));
    }

    public PartitionsBuilder fromRoot(Consumer<Splitter> splits) {
        rootFirst("root");
        splits.accept(new Splitter("root", ""));
        return this;
    }

    public PartitionsBuilder splitToNewChildren(
            String parentId, String leftId, String rightId, Object splitPoint) {
        return splitToNewChildrenOnDimension(parentId, leftId, rightId, 0, splitPoint);
    }

    public PartitionsBuilder splitToNewChildrenOnDimension(
            String parentId, String leftId, String rightId, int dimension, Object splitPoint) {
        Partition parent = partitionById(parentId).build();
        List<Partition.Builder> children = factory.split(parent, leftId, rightId, dimension, splitPoint);
        children.forEach(this::add);
        return this;
    }

    private Partition.Builder add(Partition.Builder partition) {
        partitions.add(partition);
        partitionById.put(partition.getId(), partition);
        return partition;
    }

    private Partition.Builder partitionById(String id) {
        return Optional.ofNullable(partitionById.get(id))
                .orElseThrow(() -> new IllegalArgumentException("Partition not specified: " + id));
    }

    public List<Partition> buildList() {
        return new ArrayList<>(partitions.stream().map(Partition.Builder::build).collect(Collectors.toList()));
    }

    public PartitionTree buildTree() {
        return new PartitionTree(schema, partitions.stream().map(Partition.Builder::build).collect(Collectors.toList()));
    }

    public class Splitter {

        private final String parentId;
        private final String partitionPrefix;

        private Splitter(String parentId, String partitionPrefix) {
            this.parentId = parentId;
            this.partitionPrefix = partitionPrefix;
        }

        private Splitter(String nonRootParentId) {
            this(nonRootParentId, nonRootParentId);
        }

        public void split(Object splitPoint) {
            splitToNewChildren(parentId, leftId(), rightId(), splitPoint);
        }

        public void splitToLeftAndRight(Object splitPoint,
                                        Consumer<Splitter> left,
                                        Consumer<Splitter> right) {
            split(splitPoint);
            left.accept(new Splitter(leftId()));
            right.accept(new Splitter(rightId()));
        }

        private String leftId() {
            return partitionPrefix + "L";
        }

        private String rightId() {
            return partitionPrefix + "R";
        }
    }
}
