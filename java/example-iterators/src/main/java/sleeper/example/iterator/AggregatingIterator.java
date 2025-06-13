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
package sleeper.example.iterator;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implements a record aggregating iterator similiar to the DataFusion
 * aggregation functionality.
 *
 * The format for the iterator's configuration string mirrors that of the
 * DataFusion code. It should follow this format:
 * {@code <extra aggregation columns>;<filter>,<aggregation>}. Any of the
 * components may be empty. The extra aggregation
 * columns should be a comma separated list of columns beyond the row_key
 * columns to aggregate over. Note the semi-colon
 * after the last column before the filter component. The filter component will
 * expose one possible filter: age off.
 * Only one filter will be specifiable at a time on a table (this is a minimum
 * reasonable effort!).
 * <ul>
 * <li>Age off filter format is `ageoff='column','time_period'. If the elapsed
 * time from the integer value in the given
 * column to "now" is lower than the time_period value, the row is retained.
 * Values in seconds.</li>
 * <li>Aggregation filter format is OP(column_name) where OP is one of sum, min,
 * max, map_sum, map_min or map_max.</li>
 * </ul>
 *
 * This class is not designed to be the final design and as such we expect its
 * API to change in the future in incompatible ways.
 *
 * We may decide to expand the list of allowable operations. Additional
 * aggregation filters should be comma separated.
 */
public class AggregatingIterator implements SortedRecordIterator {
    /** Pattern to match aggregation functions, e.g. "SUM(my_column)". */
    public static final Pattern AGGREGATE_REGEX = Pattern.compile("(\\w+)\\((\\w+)\\)");

    private FilterAggregationConfig config;
    private Schema schema;

    /** Aggregation functions this iterator can perform. */
    public static enum AggregationOp {
        SUM,
        MIN,
        MAX,
        MAP_SUM,
        MAP_MIN,
        MAP_MAX,
    }

    /** Defines an aggregation operation with the column name to perform on it. */
    public static record Aggregation(String column, AggregationOp op) {
        public Aggregation {
            Objects.requireNonNull(column, "column");
        }
    }

    /**
     * Defines the filtering and aggregation configuration for this iterator.
     */
    public static record FilterAggregationConfig(List<String> groupingColumns, Optional<String> ageOffColumn, long maxAge, List<Aggregation> aggregations) {
        public FilterAggregationConfig {
            Objects.requireNonNull(groupingColumns, "groupingColumns");
            Objects.requireNonNull(ageOffColumn, "ageOffColumn");
            Objects.requireNonNull(aggregations, "aggregations");
        }
    }

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> arg0) {
        if (config == null) {
            throw new IllegalStateException("AggregatingIterator has not been initialised, call init()");
        }
        // First, see if we need to age-off data
        CloseableIterator<Record> input = config.ageOffColumn().map(filter_col -> {
            AgeOffIterator ageoff = new AgeOffIterator();
            ageoff.init(String.format("%s,%d", filter_col, config.maxAge() * 1000), schema);
            return ageoff.apply(arg0);
        }).orElse(arg0);

        return input;
    }

    /**
     * Configures the iterator.
     *
     * The configuration string will be validated and the internal state of the
     * iterator will be initialised.
     *
     * @throws IllegalArgumentException if {@code configString} is not a valid
     *                                  configuration
     */
    @Override
    public void init(String configString, Schema schema) {
        FilterAggregationConfig iteratorConfig = parseConfiguration(configString);
        validate(iteratorConfig, schema);
        this.config = iteratorConfig;
        this.schema = schema;
    }

    /**
     * Performs validation on a {@link FilterAggregationConfig}.
     *
     * Validates that <strong>either</strong>:
     * <ul>
     * <li>No aggregations have been specified</li>
     * </ul>
     * OR:
     * <ol>
     * <li>All columns that are NOT query aggregation columns have an aggregation
     * operation specified for them.</li>
     * <li>No query aggregation columns have aggregations specified.</li>
     * <li>No query aggregation column is duplicated.</li>
     * <li>Aggregation columns must be valid in schema.</li>
     * </ol>
     *
     * @param  iteratorConfig           the configuration to check
     * @param  schema                   the record schema to check against
     * @throws IllegalArgumentException if {@code iteratorConfig} is invalid
     */
    private static void validate(FilterAggregationConfig iteratorConfig, Schema schema) {
        System.out.println(iteratorConfig);
        // If there are no aggregations to perform, then this configuration is trivially valid
        if (iteratorConfig.aggregations().isEmpty()) {
            return;
        }
        // Check grouping columns are not already row key columns, are not duplicated and are valid
        List<String> allColumns = schema.getAllFieldNames();
        Set<String> allGroupingColumns = new HashSet<>(
                schema.getRowKeyFieldNames());
        for (String col : iteratorConfig.groupingColumns()) {
            // Duplicated?
            if (!allGroupingColumns.add(col)) {
                throw new IllegalArgumentException("Aggregation grouping column " + col + " is already a row key column or is duplicated");
            }
            // Is valid?
            if (!allColumns.contains(col)) {
                throw new IllegalArgumentException("Aggregation grouping column " + col + " doesn't exist");
            }
        }
        // Check every non aggregation column (i.e. row key columns and extra grouping columns) have an aggregation specified
        List<String> nonGroupingColumns = new ArrayList<>(allColumns);
        nonGroupingColumns.removeIf(allGroupingColumns::contains);
        Set<String> duplicateAggregationCheck = new HashSet<>();
        // Loop through each aggregation column
        for (String aggColumn : iteratorConfig.aggregations().stream().map(Aggregation::column).toList()) {
            if (allGroupingColumns.contains(aggColumn)) {
                throw new IllegalArgumentException("Row key/extra grouping column " + aggColumn + " cannot have an aggregation");
            }
            if (!nonGroupingColumns.contains(aggColumn)) {
                throw new IllegalArgumentException("Aggregation column " + aggColumn + " doesn't exist");
            }
            if (!duplicateAggregationCheck.add(aggColumn)) {
                throw new IllegalArgumentException("Aggregation column " + aggColumn + " duplicated");
            }
        }
        // Finally, check all non row key and extra grouping columns have an aggregation specified
        for (String column : nonGroupingColumns) {
            if (!duplicateAggregationCheck.contains(column)) {
                throw new IllegalArgumentException("Column " + column + " doesn't have a aggregation operator specified!");
            }
        }
    }

    public static void main(String[] args) {
        FilterAggregationConfig config = parseConfiguration("col3;,sum(col4),sum(col5),min(col6),map_min(col7),map_sum(col8)");
        List<Field> row_keys = List.of(new Field("col1", new IntType()), new Field("col2", new IntType()));
        List<Field> sort_cols = List.of(new Field("col3", new IntType()), new Field("col4", new IntType()));
        List<Field> value_cols = List.of(new Field("col5", new IntType()), new Field("col6", new IntType()), new Field("col7", new IntType()));
        Schema s = Schema.builder().rowKeyFields(row_keys).sortKeyFields(sort_cols).valueFields(value_cols).build();
    }

    /**
     * Parses a configuration string for aggregation.
     *
     * Note that the configuration is NOT validated! Use {@link AggregatingIterator#validate(FilterAggregationConfig)}
     * to
     * check the returned configuration is valid.
     *
     * @implNote                          This is a minimum viable parser for the configuration for
     *                                    filters/aggregators. It is a really good example of how NOT to do it.
     *                                    This routine has some odd behaviour.
     *
     * @throws   IllegalArgumentException if {@code configString} is invalid
     * @return                            an un-validated configuration for aggregation
     */
    public static FilterAggregationConfig parseConfiguration(String configString) {
        // This is a minimum viable parser for the configuration for
        // filters/aggregators.
        // It is a really good example of how NOT to do it. This routine has some odd
        // behaviour.
        String[] configParts = configString.split(";", 2);
        if (configParts.length != 2) {
            throw new IllegalArgumentException("Should be exactly one ; in aggregation configuation");
        }
        // Extract the aggregation grouping columns and trim and split the remaining
        List<String> groupingColumns = Arrays.stream(configParts[0].split(",")).map(String::strip).filter(name -> !name.isEmpty()).toList();
        // Following list needs to be mutable, hence Collectors.toList()
        List<String> filter_aggs = Arrays.stream(configParts[1].split(",")).map(String::strip).collect(Collectors.toList());
        // We only support age-off filtering
        Optional<String> filter = Optional.empty();
        long maxAge = 0;
        if (filter_aggs.size() > 0) {
            if (filter_aggs.get(0).startsWith("ageoff=")) {
                filter = Optional.of(filter_aggs.get(0).split("=", 2)[1].replace("'", ""));
                maxAge = Long.parseLong(filter_aggs.get(1).replace("'", ""));
                // Remove these first two processed elements of filter_aggs list
                // This is a really hacky implementation
                filter_aggs.subList(0, 2).clear();
            } else {
                // Just remove first element
                filter_aggs.remove(0);
            }
        }
        // We use a regular expression to extract the aggregation operation and column for each remaining part
        List<Aggregation> aggregations = new ArrayList<>();
        for (String agg : filter_aggs) {
            Matcher matcher = AGGREGATE_REGEX.matcher(agg);
            if (matcher.matches()) {
                AggregationOp op = AggregationOp.valueOf(matcher.group(1).toUpperCase(Locale.getDefault()));
                String aggCol = matcher.group(2);
                aggregations.add(new Aggregation(aggCol, op));
            }
        }
        return new FilterAggregationConfig(groupingColumns, filter, maxAge, aggregations);
    }

    @Override
    public List<String> getRequiredValueFields() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRequiredValueFields'");
    }
}
