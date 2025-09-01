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

package sleeper.query.core.model;

import sleeper.core.range.Region;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A query for rows that are within the range of a leaf partition. The
 * query contains information about which files should be read. Typically
 * {@link LeafPartitionQuery}s are created from a query for a set of regions.
 * That query is broken down over leaf partitions into subqueries. Each
 * subquery retrieves rows from a leaf partition and restricts the rows
 * returned to those which are within the boundaries of the leaf partition
 * and within the regions specified.
 */
public class LeafPartitionQuery {

    private final String tableId;
    private final String queryId;
    private final String subQueryId;
    private final List<Region> regions;
    private final QueryProcessingConfig processingConfig;
    private final String leafPartitionId;
    private final Region partitionRegion;
    private final List<String> files;

    private LeafPartitionQuery(Builder builder) {
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        queryId = Objects.requireNonNull(builder.queryId, "queryId must not be null");
        subQueryId = Objects.requireNonNull(builder.subQueryId, "subQueryId must not be null");
        regions = Objects.requireNonNull(builder.regions, "regions must not be null");
        processingConfig = Objects.requireNonNull(builder.processingConfig, "processingConfig must not be null");
        leafPartitionId = Objects.requireNonNull(builder.leafPartitionId, "leafPartitionId must not be null");
        partitionRegion = Objects.requireNonNull(builder.partitionRegion, "partitionRegion must not be null");
        files = Objects.requireNonNull(builder.files, "files must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableId() {
        return tableId;
    }

    public String getQueryId() {
        return queryId;
    }

    public QueryProcessingConfig getProcessingConfig() {
        return processingConfig;
    }

    public List<Map<String, String>> getStatusReportDestinations() {
        return processingConfig.getStatusReportDestinations();
    }

    public String getQueryTimeIteratorClassName() {
        return processingConfig.getQueryTimeIteratorClassName();
    }

    public String getQueryTimeIteratorConfig() {
        return processingConfig.getQueryTimeIteratorConfig();
    }

    public String getQueryTimeFilters() {
        return processingConfig.getQueryTimeFilters();
    }

    public List<String> getRequestedValueFields() {
        return processingConfig.getRequestedValueFields();
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

    /**
     * Creates a copy of this query that will include the values of the given fields in the result.
     *
     * @param  requestedValueFields a list of requested fields
     * @return                      the copy of the original query
     */
    public LeafPartitionQuery withRequestedValueFields(List<String> requestedValueFields) {
        return toBuilder().processingConfig(processingConfig.withRequestedValueFields(requestedValueFields)).build();
    }

    private Builder toBuilder() {
        return builder()
                .tableId(tableId)
                .queryId(queryId)
                .subQueryId(subQueryId)
                .regions(regions)
                .processingConfig(processingConfig)
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
        LeafPartitionQuery that = (LeafPartitionQuery) object;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(queryId, that.queryId)
                && Objects.equals(subQueryId, that.subQueryId)
                && Objects.equals(regions, that.regions)
                && Objects.equals(processingConfig, that.processingConfig)
                && Objects.equals(leafPartitionId, that.leafPartitionId)
                && Objects.equals(partitionRegion, that.partitionRegion)
                && Objects.equals(files, that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, queryId, subQueryId, regions, processingConfig, leafPartitionId, partitionRegion, files);
    }

    @Override
    public String toString() {
        return "LeafPartitionQuery{" +
                "tableId='" + tableId + '\'' +
                ", queryId='" + queryId + '\'' +
                ", subQueryId='" + subQueryId + '\'' +
                ", regions=" + regions +
                ", processingConfig=" + processingConfig +
                ", leafPartitionId='" + leafPartitionId + '\'' +
                ", partitionRegion=" + partitionRegion +
                ", files=" + files +
                '}';
    }

    /**
     * Builder for this class.
     */
    public static final class Builder {
        private String tableId;
        private String queryId;
        private String subQueryId;
        private List<Region> regions;
        private QueryProcessingConfig processingConfig;
        private String leafPartitionId;
        private Region partitionRegion;
        private List<String> files;

        private Builder() {
        }

        /**
         * Provides the parent query from which this leaf partition query is derived.
         *
         * @param  parentQuery the parent query
         * @return             the builder
         */
        public Builder parentQuery(Query parentQuery) {
            return queryId(parentQuery.getQueryId())
                    .processingConfig(parentQuery.getProcessingConfig());
        }

        /**
         * Provides the table ID for the query.
         *
         * @param  tableId the table ID
         * @return         the builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Provides the query ID.
         *
         * @param  queryId the query ID
         * @return         the builder
         */
        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        /**
         * Provides the sub-query ID.
         *
         * @param  subQueryId the sub-query ID
         * @return            the builder
         */
        public Builder subQueryId(String subQueryId) {
            this.subQueryId = subQueryId;
            return this;
        }

        /**
         * Provides the row key range regions for the query.
         *
         * @param  regions the regions
         * @return         the builder
         */
        public Builder regions(List<Region> regions) {
            this.regions = regions;
            return this;
        }

        /**
         * Provides the processing config for the query.
         *
         * @param  processingConfig the query processing config
         * @return                  the builder
         */
        public Builder processingConfig(QueryProcessingConfig processingConfig) {
            this.processingConfig = processingConfig;
            return this;
        }

        /**
         * Provides the leaf partition ID.
         *
         * @param  leafPartitionId the leaf partition ID
         * @return                 the builder
         */
        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        /**
         * Provides the partition region.
         *
         * A partition region defines a specific range of data that the partition covers.
         * For each key field, the region specifies a range of values. Only data whose key
         * field values fall within these specified ranges will be considered part of this
         * partition.
         *
         * @param  partitionRegion the partition region
         * @return                 the builder
         */
        public Builder partitionRegion(Region partitionRegion) {
            this.partitionRegion = partitionRegion;
            return this;
        }

        /**
         * Provides the files that are in the partition.
         * These files will be read to return the results for the sub query.
         *
         * @param  files the files
         * @return       the builder
         */
        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public LeafPartitionQuery build() {
            return new LeafPartitionQuery(this);
        }
    }
}
