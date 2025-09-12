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
package sleeper.core.util;

import org.apache.commons.lang3.StringUtils;

import sleeper.core.iterator.Aggregation;
import sleeper.core.iterator.AggregationOp;
import sleeper.core.iterator.FilterAggregationConfig;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** Config class for getting iterator's from the iterator factory. */
public class IteratorConfig {
    private final String iteratorClassName;
    private final String iteratorConfigString;
    private final FilterAggregationConfig filterAggregationConfig;
    private final String filters;
    private final String aggregationString;

    public IteratorConfig(Builder builder) {
        this.iteratorClassName = builder.iteratorClassName;
        this.iteratorConfigString = builder.iteratorConfigString;
        this.filterAggregationConfig = builder.filterAggregationConfig;
        this.filters = builder.filters;
        this.aggregationString = builder.aggregationString;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getIteratorConfigString() {
        return iteratorConfigString;
    }

    public FilterAggregationConfig getFilterAggregationConfig() {
        return filterAggregationConfig;
    }

    public String getFilters() {
        return filters;
    }

    public String getAggregationString() {
        return aggregationString;
    }

    /**
     * Checks if a iterator should be applied based on if a class name or filters have been set.
     *
     * @return true if the iterator should be applied, otherwise false
     */
    public boolean shouldIteratorBeApplied() {
        return iteratorClassName != null || filters != null || aggregationString != null;
    }

    /**
     * Builder for iterator config object.
     */
    public static final class Builder {
        private String iteratorClassName;
        private String iteratorConfigString;
        private FilterAggregationConfig filterAggregationConfig;
        private String filters;
        private String aggregationString;

        private Builder() {
        }

        /**
         * Sets the iterator class name.
         *
         * @param  iteratorClassName the name of the iterator class to build
         * @return                   builder for method chaining
         */
        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        /**
         * Sets the iterator config string.
         *
         * @param  iteratorConfigString the config string to be used for the iterator
         * @return                      builder for method chaining
         */
        public Builder iteratorConfigString(String iteratorConfigString) {
            this.iteratorConfigString = iteratorConfigString;
            return this;
        }

        /**
         * Sets the filterAggregationConfig.
         *
         * @param  filterAggregationConfig the config to be used for the filters
         * @return                         builder for method chaining
         */
        public Builder filterAggregationConfig(FilterAggregationConfig filterAggregationConfig) {
            this.filterAggregationConfig = filterAggregationConfig;
            return this;
        }

        /**
         * words here.
         *
         * @param  filterString      wpord
         * @param  aggregationString wods
         * @param  schema            words
         * @return                   buildewr for method chaining
         */
        public Builder filterAggregationConfig(String filterString, String aggregationString, Schema schema) {
            this.filterAggregationConfig = getConfigFromProperties(filterString, aggregationString, schema);
            return this;
        }

        /**
         * Sets the filters string.
         *
         * @param  filters the filters string to be used for the iterator
         * @return         builder for method chaining
         */
        public Builder filters(String filters) {
            this.filters = filters;
            return this;
        }

        /**
         * Sets the aggregation string.
         *
         * @param  aggregationString the aggregation string to be used for the iterator
         * @return                   builder for method chaining
         */
        public Builder aggregationString(String aggregationString) {
            this.aggregationString = aggregationString;
            return this;
        }

        public IteratorConfig build() {
            return new IteratorConfig(this);
        }
    }

    /**
     * Gets the filter aggregation config when set by table properties.
     *
     * @param  filterString      string to generate the config for the filters
     * @param  aggregationString string to generate the config for the aggregation
     * @param  schema            the Sleeper {@link Schema} of the {@link Row} objects
     * @return                   filter aggregation config to be used in an iterator
     */
    private static FilterAggregationConfig getConfigFromProperties(String filterString, String aggregationString, Schema schema) {
        List<String> groupingColumns = new ArrayList<>(schema.getRowKeyFieldNames());
        long maxAge = 0L;
        String filterName = null;

        if (!("".equals(filterString)) && filterString != null) {
            String[] filterParts = filterString.split("\\(");
            if ("ageoff".equals(filterParts[0].toLowerCase(Locale.ENGLISH))) {
                String[] filterInput = StringUtils.chop(filterParts[1]).split(","); //Chop to remove the trailing ')'
                filterName = filterInput[0];
                maxAge = Long.parseLong(filterInput[1]);
                groupingColumns.add(filterInput[0]);
            } else {
                throw new IllegalStateException("Sleeper table filter not set to match ageOff(column,age), was: " + filterParts[0]);
            }
        }
        List<Aggregation> aggregations = List.of();
        if (aggregationString != null) {
            aggregations = generateAggregationsFromProperty(aggregationString);
            validateAggregations(aggregations, schema);
        }

        return new FilterAggregationConfig(groupingColumns,
                filterName != null ? List.of(filterName) : List.of(),
                maxAge,
                aggregations);
    }

    private static List<Aggregation> generateAggregationsFromProperty(String aggregationString) {
        List<Aggregation> aggregations = new ArrayList<Aggregation>();
        String[] aggregationSplits = aggregationString.split("\\),");
        Arrays.stream(aggregationSplits).forEach(presentAggregation -> {
            AggregationString aggObject = new AggregationString(presentAggregation
                    .replaceAll("\\)", "")
                    .split("\\("));

            try {
                aggregations.add(new Aggregation(aggObject.getColumnName(),
                        AggregationOp.valueOf(aggObject.getOpName().toUpperCase(Locale.ENGLISH).replaceFirst("^MAP_", ""))));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Unable to parse operand. Operand: " + aggObject.getOpName());
            }
        });
        return aggregations;
    }

    private static void validateAggregations(List<Aggregation> aggregations, Schema schema) {
        validateNoRowKeySortKeyAggregations(aggregations, schema);
        validateNoDuplicateAggregations(aggregations);
        validateAllColumnsHaveAggregations(aggregations, schema);
    }

    private static void validateNoRowKeySortKeyAggregations(List<Aggregation> aggregations, Schema schema) {
        List<Aggregation> rowKeySortKeyViolations = aggregations.stream().filter(agg -> {
            return schema.getRowKeyFieldNames().contains(agg.column()) ||
                    schema.getSortKeyFieldNames().contains(agg.column());
        }).toList();

        if (!rowKeySortKeyViolations.isEmpty()) {
            String errStr = rowKeySortKeyViolations.stream()
                    .map(Aggregation::column)
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Column for aggregation not allowed to be a Row Key or Sort Key. Column names: " + errStr);
        }
    }

    private static void validateNoDuplicateAggregations(List<Aggregation> aggregations) {
        HashMap<String, Boolean> aggMap = new HashMap<String, Boolean>();
        aggregations.stream().forEach(aggregation -> {
            if (aggMap.putIfAbsent(aggregation.column(), Boolean.TRUE) != null) {
                throw new IllegalStateException("Not allowed duplicate columns for aggregation. Column name: " + aggregation.column());
            }
        });
    }

    private static void validateAllColumnsHaveAggregations(List<Aggregation> aggregations, Schema schema) {
        List<String> aggregationColumns = new ArrayList<String>();
        aggregations.stream().forEach(aggregation -> {
            aggregationColumns.add(aggregation.column());
        });

        if (!aggregationColumns.containsAll(schema.getValueFieldNames())) {
            String errStr = schema.getValueFieldNames().stream()
                    .filter(presCol -> !aggregationColumns.contains(presCol))
                    .collect(Collectors.joining(", "));
            throw new IllegalStateException("Not all value fields have aggregation declared. Missing columns: " + errStr);
        }
    }

    /**
     * Record to provide parse object from the aggregation string for clarity.
     *
     * @param aggregationParts array contain sepeate parts of the aggregation.
     */
    private record AggregationString(String[] aggregationParts) {

        String getOpName() {
            return aggregationParts[0];
        }

        String getColumnName() {
            return aggregationParts[1];
        }
    }

}
