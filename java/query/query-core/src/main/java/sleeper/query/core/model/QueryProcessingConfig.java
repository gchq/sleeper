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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class configures how records should be processed during a query, and how and where to publish
 * progress and results for the query.
 */
public class QueryProcessingConfig {

    private static final QueryProcessingConfig NONE = builder().build();

    private final String queryTimeIteratorClassName;
    private final String queryTimeIteratorConfig;
    private final Map<String, String> resultsPublisherConfig;
    private final List<Map<String, String>> statusReportDestinations;
    private final List<String> requestedValueFields;

    private QueryProcessingConfig(Builder builder) {
        queryTimeIteratorClassName = builder.queryTimeIteratorClassName;
        queryTimeIteratorConfig = builder.queryTimeIteratorConfig;
        resultsPublisherConfig = Objects.requireNonNull(builder.resultsPublisherConfig, "resultsPublisherConfig must not be null");
        statusReportDestinations = Objects.requireNonNull(builder.statusReportDestinations, "statusReportDestinations must not be null");
        requestedValueFields = builder.requestedValueFields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static QueryProcessingConfig none() {
        return NONE;
    }

    public String getQueryTimeIteratorClassName() {
        return queryTimeIteratorClassName;
    }

    public String getQueryTimeIteratorConfig() {
        return queryTimeIteratorConfig;
    }

    public Map<String, String> getResultsPublisherConfig() {
        return resultsPublisherConfig;
    }

    public List<Map<String, String>> getStatusReportDestinations() {
        return statusReportDestinations;
    }

    public List<String> getRequestedValueFields() {
        return requestedValueFields;
    }

    public QueryProcessingConfig withRequestedValueFields(List<String> requestedValueFields) {
        return toBuilder()
                .requestedValueFields(requestedValueFields)
                .build();
    }

    public QueryProcessingConfig withResultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
        return toBuilder()
                .resultsPublisherConfig(resultsPublisherConfig)
                .build();
    }

    public QueryProcessingConfig withStatusReportDestination(Map<String, String> statusReportDestination) {
        return toBuilder()
                .statusReportDestinations(
                        Stream.concat(statusReportDestinations.stream(), Stream.of(statusReportDestination))
                                .collect(Collectors.toUnmodifiableList()))
                .build();
    }

    private Builder toBuilder() {
        return builder()
                .queryTimeIteratorClassName(queryTimeIteratorClassName)
                .queryTimeIteratorConfig(queryTimeIteratorConfig)
                .resultsPublisherConfig(resultsPublisherConfig)
                .statusReportDestinations(statusReportDestinations)
                .requestedValueFields(requestedValueFields);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        QueryProcessingConfig that = (QueryProcessingConfig) object;
        return Objects.equals(queryTimeIteratorClassName, that.queryTimeIteratorClassName)
                && Objects.equals(queryTimeIteratorConfig, that.queryTimeIteratorConfig)
                && Objects.equals(resultsPublisherConfig, that.resultsPublisherConfig)
                && Objects.equals(statusReportDestinations, that.statusReportDestinations)
                && Objects.equals(requestedValueFields, that.requestedValueFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryTimeIteratorClassName, queryTimeIteratorConfig, resultsPublisherConfig, statusReportDestinations, requestedValueFields);
    }

    @Override
    public String toString() {
        return "QueryProcessingConfig{" +
                "queryTimeIteratorClassName='" + queryTimeIteratorClassName + '\'' +
                ", queryTimeIteratorConfig='" + queryTimeIteratorConfig + '\'' +
                ", resultsPublisherConfig=" + resultsPublisherConfig +
                ", statusReportDestinations=" + statusReportDestinations +
                ", requestedValueFields=" + requestedValueFields +
                '}';
    }

    public static final class Builder {
        private String queryTimeIteratorClassName;
        private String queryTimeIteratorConfig;
        private Map<String, String> resultsPublisherConfig = Map.of();
        private List<Map<String, String>> statusReportDestinations = List.of();
        private List<String> requestedValueFields;

        private Builder() {
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

        public Builder requestedValueFields(List<String> requestedValueFields) {
            this.requestedValueFields = requestedValueFields;
            return this;
        }

        public QueryProcessingConfig build() {
            return new QueryProcessingConfig(this);
        }
    }
}
