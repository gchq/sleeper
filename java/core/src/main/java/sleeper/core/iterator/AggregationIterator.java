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
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Set;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Implements a row aggregating iterator similiar to the DataFusion
 * aggregation functionality.
 *
 * We may decide to expand the list of allowable operations. Additional
 * aggregation filters should be comma separated.
 *
 * This class can be used in a wrapper class that configures it in a specifc way.
 *
 * @see ConfigStringAggregationFilteringIterator
 */
public class AggregationIterator implements SortedRowIterator {

    private final List<Field> groupingFields;
    private final List<Aggregation> aggregations;
    private final List<String> requiredValueFields;

    public AggregationIterator(Schema schema, List<String> groupingFieldNames, List<Aggregation> aggregations) {
        this.groupingFields = groupingFieldNames.stream().map(field -> schema.getField(field).orElseThrow()).toList();
        this.aggregations = aggregations;
        Set<String> keyFields = schema.streamRowKeysThenSortKeys().map(Field::getName).collect(toUnmodifiableSet());
        this.requiredValueFields = groupingFieldNames.stream().filter(not(keyFields::contains)).toList();
    }

    public AggregationIterator(Schema schema, List<Aggregation> aggregations) {
        this.groupingFields = schema.streamRowKeysThenSortKeys().toList();
        this.aggregations = aggregations;
        this.requiredValueFields = List.of();
    }

    @Override
    public CloseableIterator<Row> applyTransform(CloseableIterator<Row> input) {
        return new AggregatorIteratorImpl(groupingFields, aggregations, input);
    }

    @Override
    public List<String> getRequiredValueFields() {
        return requiredValueFields;
    }

}
