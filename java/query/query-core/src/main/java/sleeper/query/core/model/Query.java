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
 * A Sleeper query definition that defines the sleeper table, the region of the table to query and a unique query Id.
 */
public class Query {
    private final String tableName;
    private final String queryId;
    private final List<Region> regions;
    private final QueryProcessingConfig processingConfig;

    private Query(Builder builder) {
        processingConfig = Objects.requireNonNull(builder.processingConfig, "processingConfig must not be null");
        queryId = requireNonNull(builder.queryId, builder, "queryId field must be provided");
        tableName = requireNonNull(builder.tableName, builder, "tableName field must be provided");
        regions = requireNonNull(builder.regions, builder, "regions field must be provided");
    }

    private static <T> T requireNonNull(T obj, Builder builder, String message) {
        if (obj == null) {
            throw new QueryValidationException(builder.queryId, builder.processingConfig.getStatusReportDestinations(), message);
        }
        return obj;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public QueryProcessingConfig getProcessingConfig() {
        return processingConfig;
    }

    public String getQueryTimeIteratorClassName() {
        return processingConfig.getQueryTimeIteratorClassName();
    }

    public String getQueryTimeIteratorConfig() {
        return processingConfig.getQueryTimeIteratorConfig();
    }

    public Map<String, String> getResultsPublisherConfig() {
        return processingConfig.getResultsPublisherConfig();
    }

    public List<String> getRequestedValueFields() {
        return processingConfig.getRequestedValueFields();
    }

    public List<Map<String, String>> getStatusReportDestinations() {
        return processingConfig.getStatusReportDestinations();
    }

    /**
     * Build a query setting the QueryProcessingConfig to use value fields.
     *
     * @param  requestedValueFields value fields to apply to the QueryProcessingConfig
     * @return                      a sleeper query
     */
    public Query withRequestedValueFields(List<String> requestedValueFields) {
        return toBuilder()
                .processingConfig(processingConfig.withRequestedValueFields(requestedValueFields))
                .build();
    }

    /**
     * Build a query setting the QueryProcessingConfig to use publisher configs.
     *
     * @param  resultsPublisherConfig publisher configs to apply to the QueryProcessingConfig
     * @return                        a sleeper query
     */
    public Query withResultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
        return toBuilder()
                .processingConfig(processingConfig.withResultsPublisherConfig(resultsPublisherConfig))
                .build();
    }

    /**
     * Build a query setting the QueryProcessingConfig to use a status report destination.
     *
     * @param  statusReportDestination status report destination to apply to the QueryProcessingConfig.
     * @return                         a sleeper query
     */
    public Query withStatusReportDestination(Map<String, String> statusReportDestination) {
        return toBuilder()
                .processingConfig(processingConfig.withStatusReportDestination(statusReportDestination))
                .build();
    }

    private Builder toBuilder() {
        return builder()
                .tableName(tableName)
                .queryId(queryId)
                .regions(regions)
                .processingConfig(processingConfig);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        Query query = (Query) object;
        return Objects.equals(tableName, query.tableName) && Objects.equals(queryId, query.queryId) && Objects.equals(regions, query.regions)
                && Objects.equals(processingConfig, query.processingConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, queryId, regions, processingConfig);
    }

    @Override
    public String toString() {
        return "Query{" +
                "tableName='" + tableName + '\'' +
                ", queryId='" + queryId + '\'' +
                ", regions=" + regions +
                ", processingConfig=" + processingConfig +
                '}';
    }

    /**
     * Builder for this class.
     */
    public static final class Builder {
        private String tableName;
        private String queryId;
        private List<Region> regions;
        private QueryProcessingConfig processingConfig = QueryProcessingConfig.none();

        private Builder() {
        }

        /**
         * Provide the table name.
         *
         * @param  tableName the name of the Sleeper table
         * @return           the builder
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Provide the query ID.
         *
         * @param  queryId the id of the query
         * @return         the builder
         */
        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        /**
         * Provide the regions.
         *
         * @param  regions the sleeper row region
         * @return         the builder
         */
        public Builder regions(List<Region> regions) {
            this.regions = regions;
            return this;
        }

        /**
         * Provide the processingConfig.
         *
         * @param  processingConfig how records should be processed during a query
         * @return                  the builder
         */
        public Builder processingConfig(QueryProcessingConfig processingConfig) {
            this.processingConfig = processingConfig;
            return this;
        }

        public Query build() {
            return new Query(this);
        }
    }
}
