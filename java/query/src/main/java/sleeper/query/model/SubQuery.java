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

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SubQuery {

    private final Query parentQuery;
    private final String subQueryId;
    private final List<Region> regions;
    private final String leafPartitionId;
    private final Region partitionRegion;
    private final List<String> files;

    private SubQuery(Builder builder) {
        parentQuery = builder.parentQuery;
        subQueryId = builder.subQueryId;
        regions = builder.regions;
        leafPartitionId = builder.leafPartitionId;
        partitionRegion = builder.partitionRegion;
        files = builder.files;
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

    public List<Map<String, String>> getStatusReportDestinations() {
        return parentQuery.getStatusReportDestinations();
    }

    public Query getParentQuery() {
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

    public LeafPartitionQuery toLeafQuery() {
        return new LeafPartitionQuery(getTableName(), getQueryId(), subQueryId, regions, leafPartitionId, partitionRegion, files);
    }

    public SubQuery withRequestedValueFields(List<String> requestedValueFields) {
        parentQuery.setRequestedValueFields(requestedValueFields);
        return this;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        SubQuery subQuery = (SubQuery) object;
        return Objects.equals(parentQuery, subQuery.parentQuery) && Objects.equals(subQueryId, subQuery.subQueryId) && Objects.equals(regions, subQuery.regions) && Objects.equals(leafPartitionId, subQuery.leafPartitionId) && Objects.equals(partitionRegion, subQuery.partitionRegion) && Objects.equals(files, subQuery.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentQuery, subQueryId, regions, leafPartitionId, partitionRegion, files);
    }

    @Override
    public String toString() {
        return "SubQuery{" +
                "parentQuery=" + parentQuery +
                ", subQueryId='" + subQueryId + '\'' +
                ", regions=" + regions +
                ", leafPartitionId='" + leafPartitionId + '\'' +
                ", partitionRegion=" + partitionRegion +
                ", files=" + files +
                '}';
    }

    public static final class Builder {
        private Query parentQuery;
        private String subQueryId;
        private List<Region> regions;
        private String leafPartitionId;
        private Region partitionRegion;
        private List<String> files;

        private Builder() {
        }

        public Builder parentQuery(Query parentQuery) {
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

        public SubQuery build() {
            return new SubQuery(this);
        }
    }
}
