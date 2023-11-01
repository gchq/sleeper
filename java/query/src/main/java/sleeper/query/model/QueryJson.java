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

import com.google.gson.JsonElement;

import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class QueryJson {
    private final String tableName;
    private final String queryId;
    private final String type;
    private final List<JsonElement> regions;
    private final List<JsonElement> subQueryRegions;
    private final List<String> requestedValueFields;
    private final String queryTimeIteratorClassName;
    private final String queryTimeIteratorConfig;
    private final Map<String, String> resultsPublisherConfig;
    private final List<Map<String, String>> statusReportDestinations;
    private final String subQueryId;
    private final String leafPartitionId;
    private final JsonElement partitionRegion;
    private final List<String> files;

    private QueryJson(Builder builder) {
        tableName = builder.tableName;
        queryId = builder.queryId;
        type = builder.type;
        regions = builder.regions;
        subQueryRegions = builder.subQueryRegions;
        requestedValueFields = builder.requestedValueFields;
        queryTimeIteratorClassName = builder.queryTimeIteratorClassName;
        queryTimeIteratorConfig = builder.queryTimeIteratorConfig;
        resultsPublisherConfig = builder.resultsPublisherConfig;
        statusReportDestinations = builder.statusReportDestinations;
        subQueryId = builder.subQueryId;
        leafPartitionId = builder.leafPartitionId;
        partitionRegion = builder.partitionRegion;
        files = builder.files;
    }

    static QueryJson from(Query query, QuerySerDe.SchemaLoader schemaLoader) {
        RegionSerDe regionSerDe = regionSerDe(schemaLoader, query);
        return builder(query, regionSerDe)
                .type("Query")
                .build();
    }

    static QueryJson from(LeafPartitionQuery leafQuery, QuerySerDe.SchemaLoader schemaLoader) {
        if (null == leafQuery.getTableName()) {
            throw new QueryValidationException(leafQuery.getQueryId(), leafQuery.getStatusReportDestinations(), "Table must not be null");
        }

        RegionSerDe regionSerDe = regionSerDe(schemaLoader, leafQuery.getParentQuery());
        return builder(leafQuery.getParentQuery(), regionSerDe)
                .type("LeafPartitionQuery")
                .subQueryId(leafQuery.getSubQueryId())
                .subQueryRegions(leafQuery.getRegions().stream()
                        .map(regionSerDe::toJsonTree)
                        .collect(Collectors.toUnmodifiableList()))
                .leafPartitionId(leafQuery.getLeafPartitionId())
                .partitionRegion(regionSerDe.toJsonTree(leafQuery.getPartitionRegion()))
                .files(leafQuery.getFiles())
                .build();
    }

    QueryOrLeafQuery toQueryOrLeafQuery(QuerySerDe.SchemaLoader schemaLoader) {
        validate();
        RegionSerDe regionSerDe = regionSerDe(schemaLoader);
        switch (type) {
            case "Query":
                return new QueryOrLeafQuery(toParentQuery(regionSerDe));
            case "LeafPartitionQuery":
                return new QueryOrLeafQuery(toLeafQuery(regionSerDe));
            default:
                throw new QueryValidationException(queryId, statusReportDestinations, "Unknown query type \"" + type + "\"");
        }
    }

    Query toParentQuery(QuerySerDe.SchemaLoader schemaLoader) {
        validate();
        return toParentQuery(regionSerDe(schemaLoader));
    }

    private Query toParentQuery(RegionSerDe regionSerDe) {
        return Query.builder()
                .tableName(tableName)
                .queryId(queryId)
                .regions(readRegions(regions, regionSerDe))
                .processingConfig(readQueryProcessingConfig())
                .build();
    }

    private QueryProcessingConfig readQueryProcessingConfig() {
        return QueryProcessingConfig.builder()
                .queryTimeIteratorClassName(queryTimeIteratorClassName)
                .queryTimeIteratorConfig(queryTimeIteratorConfig)
                .resultsPublisherConfig(Objects.requireNonNullElseGet(resultsPublisherConfig, Map::of))
                .statusReportDestinations(Objects.requireNonNullElseGet(statusReportDestinations, List::of))
                .requestedValueFields(requestedValueFields)
                .build();
    }

    private LeafPartitionQuery toLeafQuery(RegionSerDe regionSerDe) {
        Region partitionRegion = regionSerDe.fromJsonTree(this.partitionRegion);
        return LeafPartitionQuery.builder()
                .parentQuery(toParentQuery(regionSerDe))
                .subQueryId(subQueryId)
                .regions(readRegions(subQueryRegions, regionSerDe))
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(files)
                .build();
    }

    private void validate() {
        if (type == null) {
            throw new QueryValidationException(queryId, statusReportDestinations, "type field must be provided");
        }
        if (tableName == null) {
            throw new QueryValidationException(queryId, statusReportDestinations, "tableName field must be provided");
        }
    }

    private static Builder builder(Query query, RegionSerDe regionSerDe) {
        return builder()
                .tableName(query.getTableName())
                .queryId(query.getQueryId())
                .regions(query.getRegions().stream()
                        .map(regionSerDe::toJsonTree)
                        .collect(Collectors.toUnmodifiableList()))
                .requestedValueFields(query.getRequestedValueFields())
                .queryTimeIteratorClassName(query.getQueryTimeIteratorClassName())
                .queryTimeIteratorConfig(query.getQueryTimeIteratorConfig())
                .resultsPublisherConfig(query.getResultsPublisherConfig())
                .statusReportDestinations(query.getStatusReportDestinations());
    }

    private static Builder builder() {
        return new Builder();
    }

    private RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader) {
        return regionSerDe(schemaLoader, queryId, statusReportDestinations, tableName);
    }

    private static RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader, Query query) {
        return regionSerDe(schemaLoader, query.getQueryId(), query.getStatusReportDestinations(), query.getTableName());
    }

    private static RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader, String queryId, List<Map<String, String>> statusReportDestinations, String tableName) {
        return new RegionSerDe(schemaLoader.getSchema(tableName)
                .orElseThrow(() -> new QueryValidationException(queryId, statusReportDestinations,
                        "Table could not be found with name: \"" + tableName + "\"")));
    }

    private List<Region> readRegions(List<JsonElement> regions, RegionSerDe serDe) {
        if (regions == null) {
            return null;
        }
        try {
            return regions.stream()
                    .map(serDe::fromJsonTree)
                    .collect(Collectors.toUnmodifiableList());
        } catch (RegionSerDe.KeyDoesNotExistException e) {
            throw new QueryValidationException(queryId, statusReportDestinations, e);
        }
    }

    private static final class Builder {
        private String tableName;
        private String queryId;
        private String type;
        private List<JsonElement> regions;
        private List<JsonElement> subQueryRegions;
        private List<String> requestedValueFields;
        private String queryTimeIteratorClassName;
        private String queryTimeIteratorConfig;
        private Map<String, String> resultsPublisherConfig;
        private List<Map<String, String>> statusReportDestinations;
        private String subQueryId;
        private String leafPartitionId;
        private JsonElement partitionRegion;
        private List<String> files;

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder regions(List<JsonElement> regions) {
            this.regions = regions;
            return this;
        }

        public Builder subQueryRegions(List<JsonElement> subQueryRegions) {
            this.subQueryRegions = subQueryRegions;
            return this;
        }

        public Builder requestedValueFields(List<String> requestedValueFields) {
            this.requestedValueFields = requestedValueFields;
            return this;
        }

        public Builder queryTimeIteratorClassName(String queryTimeIteratorClassName) {
            this.queryTimeIteratorClassName = queryTimeIteratorClassName;
            return this;
        }

        public Builder queryTimeIteratorConfig(String queryTimeIteratorConfig) {
            this.queryTimeIteratorConfig = queryTimeIteratorConfig;
            return this;
        }

        public Builder resultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
            this.resultsPublisherConfig = resultsPublisherConfig;
            return this;
        }

        public Builder statusReportDestinations(List<Map<String, String>> statusReportDestinations) {
            this.statusReportDestinations = statusReportDestinations;
            return this;
        }

        public Builder subQueryId(String subQueryId) {
            this.subQueryId = subQueryId;
            return this;
        }

        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        public Builder partitionRegion(JsonElement partitionRegion) {
            this.partitionRegion = partitionRegion;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public QueryJson build() {
            return new QueryJson(this);
        }
    }
}
