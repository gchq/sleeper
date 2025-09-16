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
package sleeper.core.iterator;

import org.apache.commons.lang3.StringUtils;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Defines an aggregation operation with the column name to perform on it.
 *
 * @param column the column to aggregate
 * @param op     the aggregation operator
 */
public record Aggregation(String column, AggregationOp op) {

    public Aggregation {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(op, "aggregationOp");
    }

    /**
     * Parses an aggregation configuration string, and validates against a table schema.
     *
     * @param  configString the configuration string
     * @param  schema       the schema
     * @return              the configuration
     */
    public static List<Aggregation> parseConfig(String configString, Schema schema) {
        List<Aggregation> aggregations = new ArrayList<Aggregation>();
        String[] aggregationSplits = StringUtils.deleteWhitespace(configString).split("\\),");
        Arrays.stream(aggregationSplits).forEach(presentAggregation -> {
            AggregationString aggObject = new AggregationString(presentAggregation
                    .replaceAll("\\)", "")
                    .split("\\("));
            try {
                aggregations.add(new Aggregation(aggObject.getColumnName(),
                        AggregationOp.valueOf(aggObject.getOpName().toUpperCase(Locale.ENGLISH).replaceFirst("^MAP_", ""))));
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Unable to parse operand. Operand: " + aggObject.getOpName(), e);
            }
        });
        validateAggregations(aggregations, schema);
        return aggregations;
    }

    private static void validateAggregations(List<Aggregation> aggregations, Schema schema) {
        validateNoRowKeySortKeyAggregations(aggregations, schema);
        validateNoDuplicateAggregations(aggregations);
        validateAggregatedColumnsMatchValueColumns(aggregations, schema);
    }

    private static void validateNoRowKeySortKeyAggregations(List<Aggregation> aggregations, Schema schema) {
        Set<String> keyFields = schema.streamKeyFields()
                .map(Field::getName)
                .collect(toUnmodifiableSet());
        List<Aggregation> rowKeySortKeyViolations = aggregations.stream().filter(agg -> keyFields.contains(agg.column())).toList();

        if (!rowKeySortKeyViolations.isEmpty()) {
            String errStr = rowKeySortKeyViolations.stream()
                    .map(Aggregation::column)
                    .collect(Collectors.joining(", "));
            throw new IllegalArgumentException("Column for aggregation not allowed to be a Row Key or Sort Key. Column names: " + errStr);
        }
    }

    private static void validateNoDuplicateAggregations(List<Aggregation> aggregations) {
        HashMap<String, Boolean> aggMap = new HashMap<String, Boolean>();
        aggregations.stream().forEach(aggregation -> {
            if (aggMap.putIfAbsent(aggregation.column(), Boolean.TRUE) != null) {
                throw new IllegalArgumentException("Not allowed duplicate columns for aggregation. Column name: " + aggregation.column());
            }
        });
    }

    private static void validateAggregatedColumnsMatchValueColumns(List<Aggregation> aggregations, Schema schema) {
        Set<String> aggregationColumns = aggregations.stream().map(Aggregation::column).collect(toUnmodifiableSet());
        Set<String> valueFields = schema.getValueFields().stream().map(Field::getName).collect(toUnmodifiableSet());

        if (!aggregationColumns.containsAll(valueFields)) {
            String errStr = schema.getValueFields().stream().map(Field::getName)
                    .filter(presCol -> !aggregationColumns.contains(presCol))
                    .collect(joining(", "));
            throw new IllegalArgumentException("Not all value fields have aggregation declared. Missing columns: " + errStr);
        }
        if (!valueFields.containsAll(aggregationColumns)) {
            String errStr = aggregations.stream().map(Aggregation::column)
                    .filter(aggregationColumn -> !valueFields.contains(aggregationColumn))
                    .collect(joining(", "));
            throw new IllegalArgumentException("Not all aggregated fields are declared in the schema. Missing fields: " + errStr);
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
