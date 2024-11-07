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

package sleeper.query.core.model;

import com.google.gson.JsonElement;

import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

class QueryJson {
    private final String tableName;
    private final String tableId;
    private final String queryId;
    private final String type;
    private final List<JsonElement> regions;
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
        tableId = builder.tableId;
        queryId = builder.queryId;
        type = builder.type;
        regions = builder.regions;
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
        return builder()
                .type("Query")
                .tableName(query.getTableName())
                .queryId(query.getQueryId())
                .regions(writeRegions(query.getRegions(), regionSerDe))
                .processingConfig(query.getProcessingConfig())
                .build();
    }

    static QueryJson from(LeafPartitionQuery leafQuery, QuerySerDe.SchemaLoader schemaLoader) {
        RegionSerDe regionSerDe = regionSerDe(schemaLoader, leafQuery);
        return builder()
                .type("LeafPartitionQuery")
                .tableId(leafQuery.getTableId())
                .queryId(leafQuery.getQueryId())
                .subQueryId(leafQuery.getSubQueryId())
                .regions(writeRegions(leafQuery.getRegions(), regionSerDe))
                .processingConfig(leafQuery.getProcessingConfig())
                .leafPartitionId(leafQuery.getLeafPartitionId())
                .partitionRegion(regionSerDe.toJsonTree(leafQuery.getPartitionRegion()))
                .files(leafQuery.getFiles())
                .build();
    }

    QueryOrLeafPartitionQuery toQueryOrLeafQuery(QuerySerDe.SchemaLoader schemaLoader) {
        if (type == null) {
            throw new QueryValidationException(queryId, statusReportDestinations, "type field must be provided");
        }
        switch (type) {
            case "Query":
                return new QueryOrLeafPartitionQuery(toParentQuery(regionSerDeByName(schemaLoader)));
            case "LeafPartitionQuery":
                return new QueryOrLeafPartitionQuery(toLeafQuery(regionSerDeById(schemaLoader)));
            default:
                throw new QueryValidationException(queryId, statusReportDestinations, "Unknown query type \"" + type + "\"");
        }
    }

    Query toParentQuery(QuerySerDe.SchemaLoader schemaLoader) {
        return toParentQuery(regionSerDeByName(schemaLoader));
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
                .tableId(tableId)
                .queryId(queryId)
                .subQueryId(subQueryId)
                .regions(readRegions(regions, regionSerDe))
                .processingConfig(readQueryProcessingConfig())
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(files)
                .build();
    }

    private static Builder builder() {
        return new Builder();
    }

    private RegionSerDe regionSerDeByName(QuerySerDe.SchemaLoader schemaLoader) {
        if (tableName == null) {
            throw new QueryValidationException(queryId, statusReportDestinations, "tableName field must be provided");
        }
        return regionSerDeByName(schemaLoader, queryId, statusReportDestinations, tableName);
    }

    private RegionSerDe regionSerDeById(QuerySerDe.SchemaLoader schemaLoader) {
        if (tableId == null) {
            throw new QueryValidationException(queryId, statusReportDestinations, "tableId field must be provided");
        }
        return regionSerDeById(schemaLoader, queryId, statusReportDestinations, tableId);
    }

    private static RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader, Query query) {
        return regionSerDeByName(schemaLoader, query.getQueryId(), query.getStatusReportDestinations(), query.getTableName());
    }

    private static RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader, LeafPartitionQuery query) {
        return regionSerDeById(schemaLoader, query.getQueryId(), query.getStatusReportDestinations(), query.getTableId());
    }

    private static RegionSerDe regionSerDeByName(QuerySerDe.SchemaLoader schemaLoader, String queryId, List<Map<String, String>> statusReportDestinations, String tableName) {
        return new RegionSerDe(schemaLoader.getSchemaByTableName(tableName)
                .orElseThrow(() -> new QueryValidationException(queryId, statusReportDestinations,
                        "Table could not be found with name: \"" + tableName + "\"")));
    }

    private static RegionSerDe regionSerDeById(QuerySerDe.SchemaLoader schemaLoader, String queryId, List<Map<String, String>> statusReportDestinations, String tableId) {
        return new RegionSerDe(schemaLoader.getSchemaByTableId(tableId)
                .orElseThrow(() -> new QueryValidationException(queryId, statusReportDestinations,
                        "Table could not be found with ID: \"" + tableId + "\"")));
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

    private static List<JsonElement> writeRegions(List<Region> regions, RegionSerDe serDe) {
        return regions.stream()
                .map(serDe::toJsonTree)
                .collect(Collectors.toUnmodifiableList());
    }

    private static final class Builder {
        private String tableName;
        private String tableId;
        private String queryId;
        private String type;
        private List<JsonElement> regions;
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

        public Builder tableId(String tableId) {
            this.tableId = tableId;
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

        public Builder processingConfig(QueryProcessingConfig processingConfig) {
            return requestedValueFields(processingConfig.getRequestedValueFields())
                    .queryTimeIteratorClassName(processingConfig.getQueryTimeIteratorClassName())
                    .queryTimeIteratorConfig(processingConfig.getQueryTimeIteratorConfig())
                    .resultsPublisherConfig(processingConfig.getResultsPublisherConfig())
                    .statusReportDestinations(processingConfig.getStatusReportDestinations());
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
