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

public class QueryNew {
    private final String tableName;
    private final String queryId;
    private final List<Region> regions;
    private final QueryProcessingConfig processingConfig;

    private QueryNew(Builder builder) {
        tableName = builder.tableName;
        queryId = builder.queryId;
        regions = builder.regions;
        processingConfig = builder.processingConfig;
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

    public Query toOld() {
        Query query = new Query(tableName, queryId, regions);
        query.setQueryTimeIteratorClassName(getQueryTimeIteratorClassName());
        query.setQueryTimeIteratorConfig(getQueryTimeIteratorConfig());
        query.setResultsPublisherConfig(getResultsPublisherConfig());
        query.setRequestedValueFields(getRequestedValueFields());
        query.setStatusReportDestinations(getStatusReportDestinations());
        return query;
    }

    public QueryNew withRequestedValueFields(List<String> requestedValueFields) {
        return toBuilder()
                .processingConfig(processingConfig.withRequestedValueFields(requestedValueFields))
                .build();
    }

    private Builder toBuilder() {
        return builder()
                .tableName(tableName)
                .queryId(queryId)
                .regions(regions)
                .processingConfig(processingConfig);
    }

    public static final class Builder {
        private String tableName;
        private String queryId;
        private List<Region> regions;
        private QueryProcessingConfig processingConfig;

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

        public Builder regions(List<Region> regions) {
            this.regions = regions;
            return this;
        }

        public Builder processingConfig(QueryProcessingConfig processingConfig) {
            this.processingConfig = processingConfig;
            return this;
        }

        public QueryNew build() {
            return new QueryNew(this);
        }
    }
}
