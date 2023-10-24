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

import java.util.Collections;
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
public class LeafPartitionQuery extends Query {
    private final SubQueryDetails subQueryDetails;

    public LeafPartitionQuery(
            String tableName,
            String queryId,
            String subQueryId,
            List<Region> regions,
            String leafPartitionId,
            Region partitionRegion,
            List<String> files) {
        super(tableName, queryId, regions);
        subQueryDetails = SubQueryDetails.builder()
                .subQueryId(subQueryId)
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(files)
                .build();
    }

    public String getSubQueryId() {
        return subQueryDetails.getSubQueryId();
    }

    public String getLeafPartitionId() {
        return subQueryDetails.getLeafPartitionId();
    }

    public Region getPartitionRegion() {
        return subQueryDetails.getPartitionRegion();
    }

    public List<String> getFiles() {
        return subQueryDetails.getFiles();
    }

    @Override
    public String toString() {
        return "LeafPartitionQuery{"
                + "tableName=" + tableName
                + ", queryId=" + queryId
                + ", regions=" + regions
                + ", queryTimeIteratorClassName=" + queryTimeIteratorClassName
                + ", queryTimeIteratorConfig=" + queryTimeIteratorConfig
                + ", resultsPublisherConfig=" + resultsPublisherConfig
                + ", statusReportDestinations=" + statusReportDestinations
                + ", requestedValueFields=" + requestedValueFields
                + ", subQueryDetails=" + subQueryDetails + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 71 * hash + super.hashCode();
        hash = 71 * hash + subQueryDetails.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LeafPartitionQuery other = (LeafPartitionQuery) obj;
        if (!super.equals(other)) {
            return false;
        }
        return Objects.equals(this.subQueryDetails, other.subQueryDetails);
    }

    public static class Builder {
        private final LeafPartitionQuery query;

        public Builder(
                String tableName,
                String queryId,
                String subQueryId,
                List<Region> regions,
                String leafPartitionId,
                Region partitionRegion,
                List<String> files) {
            this.query = new LeafPartitionQuery(tableName, queryId, subQueryId, regions, leafPartitionId, partitionRegion, files);
        }

        public Builder(
                String tableName,
                String queryId,
                String subQueryId,
                Region region,
                String leafPartitionId,
                Region partitionRegion,
                List<String> files) {
            this(tableName,
                    queryId,
                    subQueryId,
                    Collections.singletonList(region),
                    leafPartitionId,
                    partitionRegion,
                    files);
        }

        public Builder setQueryTimeIteratorClassName(String queryTimeIteratorClassName) {
            query.setQueryTimeIteratorClassName(queryTimeIteratorClassName);
            return this;
        }

        public Builder setQueryTimeIteratorConfig(String queryTimeIteratorConfig) {
            query.setQueryTimeIteratorConfig(queryTimeIteratorConfig);
            return this;
        }

        public Builder setResultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
            query.setResultsPublisherConfig(resultsPublisherConfig);
            return this;
        }

        public Builder setRequestedValueFields(List<String> requestedValueFields) {
            query.setRequestedValueFields(requestedValueFields);
            return this;
        }

        public Builder setStatusReportDestinations(List<Map<String, String>> statusReportDestinations) {
            query.setStatusReportDestinations(statusReportDestinations);
            return this;
        }

        public LeafPartitionQuery build() {
            return query;
        }
    }
}
