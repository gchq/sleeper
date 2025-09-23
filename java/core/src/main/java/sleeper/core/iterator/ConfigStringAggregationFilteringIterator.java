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
 * {@code <extra aggregation fields>;<filter>,<aggregation>}.
 * <p>
 * Any of the components may be empty. The extra aggregation
 * fields should be a comma separated list of fields beyond the row_key
 * fields to aggregate over. Note the semi-colon
 * after the last field before the filter component. The filter component will
 * expose one possible filter: age off.
 * <p>
 * Only one filter will be specifiable at a time on a table (this is a minimum
 * reasonable effort!).
 * <ul>
 * <li>Age off filter format is `ageoff='field','time_period'`. If the elapsed
 * time from the integer value in the given
 * field to "now" is lower than the time_period value, the row is retained.
 * Values in seconds.</li>
 * <li>Aggregation filter format is OP(field_name) where OP is one of sum, min,
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
    /** Pattern to match aggregation functions, e.g. "SUM(my_field)". */
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
        iteratorConfig.ageOffFilter().map(filter -> new AgeOffIterator(schema, filter)).ifPresent(iterators::add);
        if (!iteratorConfig.aggregations().isEmpty()) {
            iterators.add(new AggregationIterator(schema, iteratorConfig.groupingColumns(), iteratorConfig.aggregations()));
        }
        iterator = new SortedRowIterators(iterators);
    }

    private static FilterAggregationConfig parseConfiguration(String configString, List<String> rowkeyNames) {
        // This is a minimum viable parser for the configuration for
        // filters/aggregators.
        // It is a really good example of how NOT to do it. This routine has some odd
        // behaviour.
        String[] configParts = configString.split(";", 2);
        if (configParts.length != 2) {
            throw new IllegalArgumentException("Should be exactly one ; in aggregation configuation");
        }
        // Extract the aggregation grouping fields and trim and split the remaining
        Stream<String> splitGroupFields = Arrays.stream(configParts[0].split(",")).map(String::strip).filter(name -> !name.isEmpty());
        List<String> groupingFields = Stream.concat(rowkeyNames.stream(), splitGroupFields).toList();

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
        // We use a regular expression to extract the aggregation operation and field for each remaining part
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
        return new FilterAggregationConfig(groupingFields, filter, maxAge, aggregations);
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
     * <li>All fields that are NOT query aggregation fields have an aggregation
     * operation specified for them.</li>
     * <li>No query aggregation fields have aggregations specified.</li>
     * <li>No query aggregation field is duplicated.</li>
     * <li>Aggregation fields must be valid in schema.</li>
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
        // Check grouping fields are not already row key fields, are not duplicated and are valid
        List<String> allFields = schema.getAllFieldNames();
        Set<String> allGroupingFields = new HashSet<>();
        for (String col : iteratorConfig.groupingColumns()) {
            // Duplicated?
            if (!allGroupingFields.add(col)) {
                throw new IllegalArgumentException("Aggregation grouping field " + col + " is already a row key field or is duplicated");
            }
            // Is valid?
            if (!allFields.contains(col)) {
                throw new IllegalArgumentException("Aggregation grouping field " + col + " doesn't exist");
            }
        }
        // Check every non aggregation field (i.e. row key fields and extra grouping fields) have an aggregation specified
        List<String> nonGroupingFields = new ArrayList<>(allFields);
        nonGroupingFields.removeIf(allGroupingFields::contains);
        Set<String> duplicateAggregationCheck = new HashSet<>();
        // Loop through each aggregation field
        for (String aggFieldName : iteratorConfig.aggregations().stream().map(Aggregation::fieldName).toList()) {
            if (allGroupingFields.contains(aggFieldName)) {
                throw new IllegalArgumentException("Row key/extra grouping field " + aggFieldName + " cannot have an aggregation");
            }
            if (!nonGroupingFields.contains(aggFieldName)) {
                throw new IllegalArgumentException("Aggregation field " + aggFieldName + " doesn't exist");
            }
            if (!duplicateAggregationCheck.add(aggFieldName)) {
                throw new IllegalArgumentException("Aggregation field " + aggFieldName + " duplicated");
            }
        }
        // Finally, check all non row key and extra grouping fields have an aggregation specified
        for (String field : nonGroupingFields) {
            if (!duplicateAggregationCheck.contains(field)) {
                throw new IllegalArgumentException("Field " + field + " doesn't have a aggregation operator specified");
            }
        }
    }
}
