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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link Query} is a request for records with rowkeys that fall within one of
 * a list of {@link Region}s.
 */
public class Query {
    protected final String tableName;
    protected final String queryId;
    protected final List<Region> regions;
    protected String queryTimeIteratorClassName;
    protected String queryTimeIteratorConfig;
    protected Map<String, String> resultsPublisherConfig;
    protected List<Map<String, String>> statusReportDestinations;
    protected List<String> requestedValueFields;

    public Query(String tableName, String queryId, List<Region> regions) {
        this.tableName = tableName;
        this.queryId = queryId;
        this.regions = regions;
        this.resultsPublisherConfig = Collections.emptyMap();
        this.statusReportDestinations = Collections.emptyList();
    }

    public String getQueryId() {
        return queryId;
    }

    public List<Region> getRegions() {
        return regions;
    }

    public void setQueryTimeIteratorClassName(String queryTimeIteratorClassName) {
        this.queryTimeIteratorClassName = queryTimeIteratorClassName;
    }

    public String getQueryTimeIteratorClassName() {
        return queryTimeIteratorClassName;
    }

    public void setQueryTimeIteratorConfig(String queryTimeIteratorConfig) {
        this.queryTimeIteratorConfig = queryTimeIteratorConfig;
    }

    public String getQueryTimeIteratorConfig() {
        return queryTimeIteratorConfig;
    }

    public void setResultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
        this.resultsPublisherConfig = resultsPublisherConfig;
    }

    public Map<String, String> getResultsPublisherConfig() {
        return resultsPublisherConfig;
    }

    public String getTableName() {
        return tableName;
    }

    public void setRequestedValueFields(List<String> requestedValueFields) {
        this.requestedValueFields = requestedValueFields;
    }

    public List<String> getRequestedValueFields() {
        return requestedValueFields;
    }

    public List<Map<String, String>> getStatusReportDestinations() {
        return statusReportDestinations;
    }

    public void setStatusReportDestinations(List<Map<String, String>> statusReportDestinations) {
        this.statusReportDestinations = statusReportDestinations;
    }

    public void addStatusReportDestination(Map<String, String> statusReportDestination) {
        this.statusReportDestinations.add(statusReportDestination);
    }

    public QueryNew toNew() {
        return QueryNew.builder()
                .tableName(tableName)
                .queryId(queryId)
                .regions(regions)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName(queryTimeIteratorClassName)
                        .queryTimeIteratorConfig(queryTimeIteratorConfig)
                        .resultsPublisherConfig(Objects.requireNonNullElseGet(resultsPublisherConfig, Map::of))
                        .statusReportDestinations(Objects.requireNonNullElseGet(statusReportDestinations, List::of))
                        .requestedValueFields(requestedValueFields)
                        .build())
                .build();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + Objects.hashCode(this.tableName);
        hash = 67 * hash + Objects.hashCode(this.queryId);
        hash = 67 * hash + Objects.hashCode(this.regions);
        hash = 67 * hash + Objects.hashCode(this.queryTimeIteratorClassName);
        hash = 67 * hash + Objects.hashCode(this.queryTimeIteratorConfig);
        hash = 67 * hash + Objects.hashCode(this.resultsPublisherConfig);
        hash = 67 * hash + Objects.hashCode(this.requestedValueFields);
        hash = 67 * hash + Objects.hashCode(new HashSet<>(this.statusReportDestinations));
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
        final Query other = (Query) obj;
        if (!Objects.equals(this.tableName, other.tableName)) {
            return false;
        }
        if (!Objects.equals(this.queryId, other.queryId)) {
            return false;
        }
        if (!Objects.equals(this.queryTimeIteratorClassName, other.queryTimeIteratorClassName)) {
            return false;
        }
        if (!Objects.equals(this.queryTimeIteratorConfig, other.queryTimeIteratorConfig)) {
            return false;
        }
        if (!Objects.equals(this.regions, other.regions)) {
            return false;
        }
        if (!Objects.equals(this.resultsPublisherConfig, other.resultsPublisherConfig)) {
            return false;
        }
        if (!Objects.equals(this.requestedValueFields, other.requestedValueFields)) {
            return false;
        }
        return Objects.equals(new HashSet<>(this.statusReportDestinations), new HashSet<>(other.statusReportDestinations));
    }

    @Override
    public String toString() {
        return "Query{"
                + "tableName=" + tableName
                + ", queryId=" + queryId
                + ", ranges=" + regions
                + ", queryTimeIteratorClassName=" + queryTimeIteratorClassName
                + ", queryTimeIteratorConfig=" + queryTimeIteratorConfig
                + ", resultsPublisherConfig=" + resultsPublisherConfig
                + ", requestedValueFields=" + requestedValueFields
                + ", statusReportDestinations=" + statusReportDestinations + '}';
    }

    public static class Builder {
        private final Query query;

        public Builder(String tableName, String queryId, List<Region> regions) {
            this.query = new Query(tableName, queryId, regions);
            validateRegions(regions);
        }

        public Builder(String tableName, String queryId, Region region) {
            this(tableName, queryId, Collections.singletonList(region));
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

        public Query build() {
            return query;
        }
    }

    private static void validateRegions(List<Region> regions) {
        if (null == regions) {
            throw new IllegalArgumentException("Regions cannot be null");
        }
        if (regions.isEmpty()) {
            return;
        }
        for (Region region : regions) {
            if (region.getRanges().isEmpty()) {
                throw new IllegalArgumentException("Each region must contain at least one range");
            }
        }
    }
}
