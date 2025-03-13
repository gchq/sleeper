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

import sleeper.core.key.Key;
import sleeper.core.range.Region;
import sleeper.core.range.RegionCanonicaliser;
import sleeper.core.schema.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A Partition is a Region of key space, with additional information that allows
 * the position of the partition in the tree of partitions to be identified,
 * e.g. it has a unique id, the id of its parent partition, the id of any child
 * partitions, and if it has been split the dimension it was split on.
 * <p>
 * The {@link Region} must be in canonical form, i.e. all the ranges must contain
 * their minimum but not contain their maximum.
 */
public class Partition {
    private final Region region;
    private final String id;
    private final boolean leafPartition;
    private final String parentPartitionId;
    private final List<String> childPartitionIds;
    private final int dimension;

    private Partition(Partition.Builder builder) {
        region = Objects.requireNonNull(builder.region, "region must not be null");
        id = Objects.requireNonNull(builder.id, "id must not be null");
        leafPartition = builder.leafPartition;
        parentPartitionId = builder.parentPartitionId;
        childPartitionIds = Optional.ofNullable(builder.childPartitionIds).orElse(Collections.emptyList());
        dimension = builder.dimension;
        if (!RegionCanonicaliser.isRegionInCanonicalForm(region)) {
            throw new IllegalArgumentException("Region must be in canonical form");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Checks if a row key is in this partition. This depends on the ranges the partition covers.
     *
     * @param  schema schema of the Sleeper table
     * @param  rowKey values of row key to check
     * @return        true if the row key is in the partition
     */
    public boolean isRowKeyInPartition(Schema schema, Key rowKey) {
        return region.isKeyInRegion(schema, rowKey);
    }

    /**
     * Checks if a region overlaps this partition.
     *
     * @param  otherRegion region to check
     * @return             true if any part of the region is in this partition
     */
    public boolean doesRegionOverlapPartition(Region otherRegion) {
        return region.doesRegionOverlap(otherRegion);
    }

    public Region getRegion() {
        return region;
    }

    public String getId() {
        return id;
    }

    public boolean isLeafPartition() {
        return leafPartition;
    }

    public String getParentPartitionId() {
        return parentPartitionId;
    }

    public List<String> getChildPartitionIds() {
        return childPartitionIds;
    }

    public int getDimension() {
        return dimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Partition partition = (Partition) o;

        return leafPartition == partition.leafPartition
                && Objects.equals(region, partition.region)
                && Objects.equals(id, partition.id)
                && Objects.equals(parentPartitionId, partition.parentPartitionId)
                && Objects.equals(childPartitionIds, partition.childPartitionIds)
                && dimension == partition.getDimension();
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, id, leafPartition,
                parentPartitionId, childPartitionIds, dimension);
    }

    @Override
    public String toString() {
        return "Partition{"
                + "region=" + region
                + ", id='" + id + '\''
                + ", leafPartition=" + leafPartition
                + ", parentPartitionId='" + parentPartitionId + '\''
                + ", childPartitionIds=" + childPartitionIds
                + ", dimension=" + dimension
                + '}';
    }

    public Builder toBuilder() {
        return builder()
                .region(region)
                .id(id)
                .leafPartition(leafPartition)
                .childPartitionIds(childPartitionIds)
                .parentPartitionId(parentPartitionId)
                .dimension(dimension);
    }

    /**
     * Builder to create a partition object.
     */
    public static final class Builder {
        private Region region;
        private String id;
        private boolean leafPartition;
        private String parentPartitionId;
        private List<String> childPartitionIds;
        private int dimension = -1; // -1 used to indicate that it has not been split yet; when it has been split, indicates which dimension was used to split on.

        private Builder() {
        }

        /**
         * Sets the region covered by the partition.
         *
         * @param  region the region
         * @return        the builder
         */
        public Builder region(Region region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the ID of the partition.
         *
         * @param  id a unique identifier
         * @return    the builder
         */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets whether this is a leaf partition.
         *
         * @param  leafPartition true if the partition has no child partitions, false otherwise
         * @return               the builder
         */
        public Builder leafPartition(boolean leafPartition) {
            this.leafPartition = leafPartition;
            return this;
        }

        /**
         * Sets the parent of the partition. This is the ID of the partition that was split to create this partition.
         * Can be null if this is the root partition of the Sleeper table.
         *
         * @param  parentPartitionId the ID of the parent partition
         * @return                   the builder
         */
        public Builder parentPartitionId(String parentPartitionId) {
            this.parentPartitionId = parentPartitionId;
            return this;
        }

        /**
         * Sets the children of the partition. This is the IDs of any partitions that were created by splitting this
         * partition. This will default to an empty list, and must be empty for a leaf partition.
         *
         * @param  childPartitionIds the IDs of the child partitions
         * @return                   the builder
         */
        public Builder childPartitionIds(List<String> childPartitionIds) {
            this.childPartitionIds = childPartitionIds;
            return this;
        }

        /**
         * Sets the dimension this partition was split on. If this partition has been split, this should be the index of
         * the row key in the schema that this partition was split on.
         *
         * @param  dimension the index of the row key used as the split point
         * @return           the builder
         */
        public Builder dimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        public Partition build() {
            return new Partition(this);
        }

        public String getId() {
            return id;
        }

        public Region getRegion() {
            return region;
        }
    }
}
