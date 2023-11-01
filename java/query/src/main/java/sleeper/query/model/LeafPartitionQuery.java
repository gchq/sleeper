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

package sleeper.query.model;

import sleeper.core.range.Region;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A query for records that are within the range of a leaf partition. The
 * query contains information about which files should be read. Typically
 * {@link LeafPartitionQuery}s are created from a query for a set of regions.
 * That query is broken down over leaf partitions into subqueries. Each
 * subquery retrieves records from a leaf partition and restricts the records
 * returned to those which are within the boundaries of the leaf partition
 * and within the regions specified.
 */
public class LeafPartitionQuery {

    private final QueryNew parentQuery;
    private final String subQueryId;
    private final List<Region> regions;
    private final String leafPartitionId;
    private final Region partitionRegion;
    private final List<String> files;

    private LeafPartitionQuery(Builder builder) {
        parentQuery = Objects.requireNonNull(builder.parentQuery, "parentQuery must not be null");
        subQueryId = Objects.requireNonNull(builder.subQueryId, "subQueryId must not be null");
        regions = Objects.requireNonNull(builder.regions, "regions must not be null");
        leafPartitionId = Objects.requireNonNull(builder.leafPartitionId, "leafPartitionId must not be null");
        partitionRegion = Objects.requireNonNull(builder.partitionRegion, "partitionRegion must not be null");
        files = Objects.requireNonNull(builder.files, "files must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getQueryId() {
        return parentQuery.getQueryId();
    }

    public String getTableName() {
        return parentQuery.getTableName();
    }

    public QueryProcessingConfig getProcessingConfig() {
        return parentQuery.getProcessingConfig();
    }

    public List<Map<String, String>> getStatusReportDestinations() {
        return parentQuery.getStatusReportDestinations();
    }

    public String getQueryTimeIteratorClassName() {
        return parentQuery.getQueryTimeIteratorClassName();
    }

    public String getQueryTimeIteratorConfig() {
        return parentQuery.getQueryTimeIteratorConfig();
    }

    public List<String> getRequestedValueFields() {
        return parentQuery.getRequestedValueFields();
    }

    public QueryNew getParentQuery() {
        return parentQuery;
    }

    public String getSubQueryId() {
        return subQueryId;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public String getLeafPartitionId() {
        return leafPartitionId;
    }

    public Region getPartitionRegion() {
        return partitionRegion;
    }

    public List<String> getFiles() {
        return files;
    }

    public LeafPartitionQuery withRequestedValueFields(List<String> requestedValueFields) {
        return toBuilder().parentQuery(parentQuery.withRequestedValueFields(requestedValueFields)).build();
    }

    private Builder toBuilder() {
        return builder()
                .parentQuery(parentQuery)
                .subQueryId(subQueryId)
                .regions(regions)
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(files);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        LeafPartitionQuery subQuery = (LeafPartitionQuery) object;
        return Objects.equals(parentQuery, subQuery.parentQuery)
                && Objects.equals(subQueryId, subQuery.subQueryId)
                && Objects.equals(regions, subQuery.regions)
                && Objects.equals(leafPartitionId, subQuery.leafPartitionId)
                && Objects.equals(partitionRegion, subQuery.partitionRegion)
                && Objects.equals(new HashSet<>(files), new HashSet<>(subQuery.files));
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentQuery, subQueryId, regions, leafPartitionId, partitionRegion, files);
    }

    @Override
    public String toString() {
        return "LeafPartitionQuery{" +
                "parentQuery=" + parentQuery +
                ", subQueryId='" + subQueryId + '\'' +
                ", regions=" + regions +
                ", leafPartitionId='" + leafPartitionId + '\'' +
                ", partitionRegion=" + partitionRegion +
                ", files=" + files +
                '}';
    }

    public static final class Builder {
        private QueryNew parentQuery;
        private String subQueryId;
        private List<Region> regions;
        private String leafPartitionId;
        private Region partitionRegion;
        private List<String> files;

        private Builder() {
        }

        public Builder parentQuery(Query parentQuery) {
            return parentQuery(parentQuery.toNew());
        }

        public Builder parentQuery(QueryNew parentQuery) {
            this.parentQuery = parentQuery;
            return regions(parentQuery.getRegions());
        }

        public Builder subQueryId(String subQueryId) {
            this.subQueryId = subQueryId;
            return this;
        }

        public Builder regions(List<Region> regions) {
            this.regions = regions;
            return this;
        }

        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        public Builder partitionRegion(Region partitionRegion) {
            this.partitionRegion = partitionRegion;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public LeafPartitionQuery build() {
            return new LeafPartitionQuery(this);
        }
    }
}
