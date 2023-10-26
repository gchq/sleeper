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
import com.google.gson.JsonParseException;

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
        if (null == query.getTableName()) {
            throw new QueryValidationException(query.getQueryId(), "Table must not be null");
        }
        RegionSerDe regionSerDe = regionSerDe(schemaLoader, query);
        if (query instanceof LeafPartitionQuery) {
            return from((LeafPartitionQuery) query, regionSerDe);
        } else {
            return builder(query, regionSerDe)
                    .type("Query")
                    .build();
        }
    }

    Query toQuery(QuerySerDe.SchemaLoader schemaLoader) {
        if (type == null) {
            throw new JsonParseException("type field must be provided");
        }
        if (queryId == null) {
            throw new JsonParseException("queryId field must be provided");
        }
        if (tableName == null) {
            throw new JsonParseException("tableName field must be provided");
        }
        RegionSerDe regionSerDe = regionSerDe(schemaLoader, queryId, tableName);
        switch (type) {
            case "Query":
                return new Query.Builder(tableName, queryId, readRegions(regionSerDe))
                        .setRequestedValueFields(requestedValueFields)
                        .setQueryTimeIteratorClassName(queryTimeIteratorClassName)
                        .setQueryTimeIteratorConfig(queryTimeIteratorConfig)
                        .setResultsPublisherConfig(resultsPublisherConfig)
                        .setStatusReportDestinations(readStatusReportDestinations())
                        .build();
            case "LeafPartitionQuery":
                Region partitionRegion = regionSerDe.fromJsonTree(this.partitionRegion);
                return new LeafPartitionQuery.Builder(
                        tableName, queryId, subQueryId, readRegions(regionSerDe), leafPartitionId, partitionRegion, files)
                        .setRequestedValueFields(requestedValueFields)
                        .setQueryTimeIteratorClassName(queryTimeIteratorClassName)
                        .setQueryTimeIteratorConfig(queryTimeIteratorConfig)
                        .setResultsPublisherConfig(resultsPublisherConfig)
                        .setStatusReportDestinations(readStatusReportDestinations())
                        .build();
            default:
                throw new QueryValidationException(queryId, "Unknown query type \"" + type + "\"");
        }
    }

    private static QueryJson from(LeafPartitionQuery query, RegionSerDe regionSerDe) {
        return builder(query, regionSerDe)
                .type("LeafPartitionQuery")
                .subQueryId(query.getSubQueryId())
                .leafPartitionId(query.getLeafPartitionId())
                .partitionRegion(regionSerDe.toJsonTree(query.getPartitionRegion()))
                .files(query.getFiles())
                .build();
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

    private static RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader, Query query) {
        return regionSerDe(schemaLoader, query.getQueryId(), query.getTableName());
    }

    private static RegionSerDe regionSerDe(QuerySerDe.SchemaLoader schemaLoader, String queryId, String tableName) {
        return new RegionSerDe(schemaLoader.getSchema(tableName)
                .orElseThrow(() -> new QueryValidationException(queryId,
                        "Table could not be found with name: \"" + tableName + "\"")));
    }

    private List<Region> readRegions(RegionSerDe serDe) {
        if (regions == null) {
            return null;
        }
        try {
            return regions.stream()
                    .map(serDe::fromJsonTree)
                    .collect(Collectors.toUnmodifiableList());
        } catch (RegionSerDe.KeyDoesNotExistException e) {
            throw new QueryValidationException(queryId, e);
        }
    }

    private List<Map<String, String>> readStatusReportDestinations() {
        return Objects.requireNonNullElseGet(statusReportDestinations, List::of);
    }

    private static final class Builder {
        private String tableName;
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
