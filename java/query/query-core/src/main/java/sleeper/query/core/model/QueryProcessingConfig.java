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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class configures how rows should be processed during a query, and how and where to publish
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

    /**
     * Creates a QueryProcessingConfig object.
     * Offers a default state for queries that do not require specific processing parameters or status reports.
     *
     * @return a query processing config object
     */
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

    /**
     * Creates a copy of this configuration that will include the values of the given fields in the query result.
     *
     * @param  requestedValueFields value fields to set in the builder
     * @return                      the copy
     */
    public QueryProcessingConfig withRequestedValueFields(List<String> requestedValueFields) {
        return toBuilder()
                .requestedValueFields(requestedValueFields)
                .build();
    }

    /**
     * Creates a copy of this configuration that includes the given configuration for publishing results.
     *
     * @param  resultsPublisherConfig results publisher config to set in the builder
     * @return                        the copy
     */
    public QueryProcessingConfig withResultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
        return toBuilder()
                .resultsPublisherConfig(resultsPublisherConfig)
                .build();
    }

    /**
     * Creates a copy of this configuration that includes the given configuration for publishing status reports.
     *
     * @param  statusReportDestination status report destination to set in the builder
     * @return                         the copy
     */
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

    /**
     * Builder for this class.
     */
    public static final class Builder {
        private String queryTimeIteratorClassName;
        private String queryTimeIteratorConfig;
        private Map<String, String> resultsPublisherConfig = Map.of();
        private List<Map<String, String>> statusReportDestinations = List.of();
        private List<String> requestedValueFields;

        private Builder() {
        }

        /**
         * Provides the query time iterator class name.
         *
         * @param  queryTimeIteratorClassName the name of the class
         * @return                            the builder
         */
        public Builder queryTimeIteratorClassName(String queryTimeIteratorClassName) {
            this.queryTimeIteratorClassName = queryTimeIteratorClassName;
            return this;
        }

        /**
         * Provides the query time iterator config.
         *
         * @param  queryTimeIteratorConfig the iterator config
         * @return                         the builder
         */
        public Builder queryTimeIteratorConfig(String queryTimeIteratorConfig) {
            this.queryTimeIteratorConfig = queryTimeIteratorConfig;
            return this;
        }

        /**
         * Provides the results publisher config.
         *
         * @param  resultsPublisherConfig the publisher config
         * @return                        the builder
         */
        public Builder resultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
            this.resultsPublisherConfig = resultsPublisherConfig;
            return this;
        }

        /**
         * Provides the status report destinations.
         *
         * @param  statusReportDestinations the report destinations
         * @return                          the builder
         */
        public Builder statusReportDestinations(List<Map<String, String>> statusReportDestinations) {
            this.statusReportDestinations = statusReportDestinations;
            return this;
        }

        /**
         * Provides the fields that are requested to be included in the query results.
         *
         * @param  requestedValueFields the value fields
         * @return                      the builder
         */
        public Builder requestedValueFields(List<String> requestedValueFields) {
            this.requestedValueFields = requestedValueFields;
            return this;
        }

        public QueryProcessingConfig build() {
            return new QueryProcessingConfig(this);
        }
    }
}
