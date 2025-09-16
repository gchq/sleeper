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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implements a row aggregating iterator similiar to the DataFusion
 * aggregation functionality.
 * <p>
 *
 * The format for the iterator's configuration string mirrors that of the
 * DataFusion code. It should follow this format:
 * <p>
 * {@code <extra aggregation columns>;<filter>,<aggregation>}.
 * <p>
 * Any of the components may be empty. The extra aggregation
 * columns should be a comma separated list of columns beyond the row_key
 * columns to aggregate over. Note the semi-colon
 * after the last column before the filter component. The filter component will
 * expose one possible filter: age off.
 * <p>
 * Only one filter will be specifiable at a time on a table (this is a minimum
 * reasonable effort!).
 * <ul>
 * <li>Age off filter format is `ageoff='column','time_period'`. If the elapsed
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
public class ConfigStringAggregationFilteringIterator implements ConfigStringIterator {
    /** Pattern to match aggregation functions, e.g. "SUM(my_column)". */
    public static final Pattern AGGREGATE_REGEX = Pattern.compile("(\\w+)\\((\\w+)\\)");

    private SortedRowIterator iterator;

    @Override
    public List<String> getRequiredValueFields() {
        return iterator.getRequiredValueFields();
    }

    @Override
    public CloseableIterator<Row> applyTransform(CloseableIterator<Row> input) {
        return iterator.applyTransform(input);
    }

    @Override
    public void init(String configString, Schema schema) {
        FilterAggregationConfig iteratorConfig = parseConfiguration(configString, schema.getRowKeyFieldNames());
        validate(iteratorConfig, schema);
        List<SortedRowIterator> iterators = new ArrayList<>();
        iteratorConfig.ageOffFilter().map(AgeOffIterator::new).ifPresent(iterators::add);
        if (!iteratorConfig.aggregations().isEmpty()) {
            iterators.add(new AggregationIterator(schema, iteratorConfig.groupingColumns(), iteratorConfig.aggregations()));
        }
        iterator = new SortedRowIterators(iterators);
    }

    /**
     * Parses a configuration string for aggregation.
     *
     * The string must be in the correct format. Row key columns are automatically "group by" columns for any
     * aggregation
     * so the list of row key columns is added into the returned {@link FilterAggregationConfig}.
     *
     * Note that the configuration is NOT validated! Use
     * {@link AggregationFilteringIterator#validate(FilterAggregationConfig)}
     * to check the returned configuration is valid.
     *
     * @implNote                          This is a minimum viable parser for the configuration for
     *                                    filters/aggregators. It is a really good example of how NOT to do it.
     *                                    This routine has some odd behaviour.
     *
     * @param    configString             the filter and aggregation string to parse
     * @param    rowkeyNames              the list of all the row key column names from the schema
     * @return                            an un-validated configuration for aggregation
     * @throws   IllegalArgumentException if {@code configString} is invalid
     */
    private static FilterAggregationConfig parseConfiguration(String configString, List<String> rowkeyNames) {
        // This is a minimum viable parser for the configuration for
        // filters/aggregators.
        // It is a really good example of how NOT to do it. This routine has some odd
        // behaviour.
        String[] configParts = configString.split(";", 2);
        if (configParts.length != 2) {
            throw new IllegalArgumentException("Should be exactly one ; in aggregation configuation");
        }
        // Extract the aggregation grouping columns and trim and split the remaining
        Stream<String> splitGroupColumns = Arrays.stream(configParts[0].split(",")).map(String::strip).filter(name -> !name.isEmpty());
        List<String> groupingColumns = Stream.concat(rowkeyNames.stream(), splitGroupColumns).toList();

        // Following list needs to be mutable, hence Collectors.toList()
        List<String> filterAggs = Arrays.stream(configParts[1].split(",")).map(String::strip).collect(Collectors.toList());
        // We only support age-off filtering
        Optional<String> filter = Optional.empty();
        long maxAge = 0;
        if (filterAggs.size() > 0) {
            if (filterAggs.get(0).startsWith("ageoff=")) {
                filter = Optional.of(filterAggs.get(0).split("=", 2)[1].replace("'", ""));
                maxAge = Long.parseLong(filterAggs.get(1).replace("'", ""));
                // Remove these first two processed elements of filter_aggs list
                // This is a really hacky implementation
                filterAggs.subList(0, 2).clear();
            } else {
                // Just remove first element
                filterAggs.remove(0);
            }
        }
        // We use a regular expression to extract the aggregation operation and column for each remaining part
        List<Aggregation> aggregations = new ArrayList<>();
        for (String agg : filterAggs) {
            Matcher matcher = AGGREGATE_REGEX.matcher(agg);
            if (matcher.matches()) {
                // We implement the "Map" variants with the same operators as the primitive ones, so we can
                // remove the "map_" prefix.
                String primitiveOp = matcher.group(1).toUpperCase(Locale.getDefault()).replaceFirst("^MAP_", "");
                AggregationOp op = AggregationOp.valueOf(primitiveOp);
                String aggCol = matcher.group(2);
                aggregations.add(new Aggregation(aggCol, op));
            }
        }
        return new FilterAggregationConfig(groupingColumns, filter, maxAge, aggregations);
    }

    /**
     * Performs validation.
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
     * @param  schema                   the schema to check against
     * @throws IllegalArgumentException if {@code iteratorConfig} is invalid
     */
    private static void validate(FilterAggregationConfig iteratorConfig, Schema schema) {
        // If there are no aggregations to perform, then this configuration is trivially valid
        if (iteratorConfig.aggregations().isEmpty()) {
            return;
        }
        // Check grouping columns are not already row key columns, are not duplicated and are valid
        List<String> allColumns = schema.getAllFieldNames();
        Set<String> allGroupingColumns = new HashSet<>();
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
                throw new IllegalArgumentException("Column " + column + " doesn't have a aggregation operator specified");
            }
        }
    }
}
