/*
 * Copyright 2022 Crown Copyright
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

import sleeper.core.key.Key;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This represents a tree of {@link Partition}s. It can be used to find all
 * ancestors of a partition, i.e. all partitions that are either parents of the
 * partition, or grandparents, or great-grandparents.
 */
public class PartitionTree {
    private final Schema schema;
    private final Map<String, Partition> idToPartition;
    private final Partition rootPartition;

    public PartitionTree(Schema schema, List<Partition> partitions) {
        this.schema = schema;
        this.idToPartition = new HashMap<>();
        partitions.forEach(p -> this.idToPartition.put(p.getId(), p));
        List<Partition> rootPartitions = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList());
        // There should be exactly one root partition.
        if (rootPartitions.size() != 1) {
            throw new IllegalArgumentException("There should be exactly one root partition, found " + rootPartitions.size());
        }
        this.rootPartition = rootPartitions.get(0);
    }

    public List<String> getChildIds(String partitionId) throws IllegalArgumentException {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }
        return idToPartition.get(partitionId).getChildPartitionIds();
    }

    public String getParentId(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }
        return idToPartition.get(partitionId).getParentPartitionId();
    }

    public List<String> getAllAncestorIds(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }

        List<String> ancestors = new ArrayList<>();
        String parent = idToPartition.get(partitionId).getParentPartitionId();
        while (null != parent) {
            ancestors.add(parent);
            parent = null == idToPartition.get(parent) ? null : idToPartition.get(parent).getParentPartitionId();
        }
        return ancestors;
    }

    public List<Partition> getAllAncestors(String partitionId) {
        if (!idToPartition.containsKey(partitionId)) {
            throw new IllegalArgumentException("No partition of id " + partitionId);
        }

        List<Partition> ancestors = new ArrayList<>();
        String parentId = idToPartition.get(partitionId).getParentPartitionId();
        while (null != parentId) {
            ancestors.add(idToPartition.get(parentId));
            parentId = idToPartition.get(parentId).getParentPartitionId();
        }
        return ancestors;
    }

    public Partition getPartition(String partitionId) {
        return idToPartition.get(partitionId);
    }

    public List<Partition> getAllPartitions() {
        return Collections.unmodifiableList(new ArrayList<>(idToPartition.values()));
    }

    public List<Partition> getLeafPartitions() {
        return Collections.unmodifiableList(idToPartition.values().stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList()));
    }

    public Partition getLeafPartition(Key key) {
        // Sanity check key is of the correct length
        if (key.size() != schema.getRowKeyFields().size()) {
            throw new IllegalArgumentException("Key must match the row key fields from the schema (key was "
                    + key + ", schema has row key fields " + schema.getRowKeyFields() + ")");
        }
        // If root partition is a leaf partition then key must be in it
        if (rootPartition.isLeafPartition()) {
            return rootPartition;
        }

        return descend(rootPartition, key);
    }

    private Partition descend(Partition currentNode, Key key) {
        // Get child partitions
        List<String> childPartitionIds = currentNode.getChildPartitionIds();
        List<Partition> childPartitions = new ArrayList<>();
        childPartitionIds.forEach(c -> childPartitions.add(idToPartition.get(c)));

        // Which child is the key in?
        Partition child = null;
        for (Partition partition : childPartitions) {
            if (partition.isRowKeyInPartition(schema, key)) {
                child = partition;
                break;
            }
        }
        if (null == child) {
            throw new IllegalArgumentException("Found key that was not in any of the child partitions: key " + key
                    + ", child partitions " + childPartitions);
        }
        if (child.isLeafPartition()) {
            return child;
        }
        return descend(child, key);
    }

    public Partition getRootPartition() {
        return rootPartition;
    }

    public Partition getNearestCommonAncestor(Key a, Key b) {
        return getNearestCommonAncestor(getLeafPartition(a), getLeafPartition(b));
    }

    public Partition getNearestCommonAncestor(Partition a, Partition b) {
        if (a.getId().equals(b.getId())) {
            return a;
        }
        Set<String> ancestorsB = new HashSet<>(getAllAncestorIds(b.getId()));
        for (String ancestorA : getAllAncestorIds(a.getId())) {
            if (ancestorsB.contains(ancestorA)) {
                return getPartition(ancestorA);
            }
        }
        return getRootPartition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionTree that = (PartitionTree) o;
        return schema.equals(that.schema)
                && idToPartition.equals(that.idToPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, idToPartition);
    }
}
