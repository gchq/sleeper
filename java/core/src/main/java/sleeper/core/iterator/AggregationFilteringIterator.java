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

import java.util.List;
import java.util.stream.Stream;

/**
 * Implements a row aggregating iterator similiar to the DataFusion
 * aggregation functionality.
 *
 * This class is not designed to be the final design and as such we expect its
 * API to change in the future in incompatible ways.
 *
 * We may decide to expand the list of allowable operations. Additional
 * aggregation filters should be comma separated.
 *
 * This class can be used in a wrapper class that configures it in a specifc way.
 *
 * @see ConfigStringAggregationFilteringIterator
 */
public class AggregationFilteringIterator implements SortedRowIterator {

    /** Combined configuration for the optional filtering and aggregation behaviour. */
    private final FilterAggregationConfig config;
    /** Table schema being filtered. */
    private final Schema schema;

    public AggregationFilteringIterator(FilterAggregationConfig config, Schema schema) {
        this.config = config;
        this.schema = schema;
    }

    @Override
    public List<String> getRequiredValueFields() {
        if (config == null) {
            throw new IllegalStateException("AggregatingIterator has not been initialised, call init()");
        }
        return Stream.concat(config.groupingColumns().stream(), config.aggregations().stream().map(Aggregation::column)).toList();
    }

    @Override
    public CloseableIterator<Row> applyTransform(CloseableIterator<Row> source) {
        if (config == null) {
            throw new IllegalStateException("AggregatingIterator has not been initialised, call init()");
        }
        // See if we need to age-off data
        CloseableIterator<Row> input = maybeCreateFilter(source);

        // Do any aggregations need to be performed?
        if (!config.aggregations().isEmpty()) {
            return new AggregatorIteratorImpl(config, input, schema);
        } else {
            return input;
        }
    }

    /**
     * Configures an age off filter on the source iterator if needed.
     *
     * If this aggregating iterator has been configured to provide age off filtering,
     * then an {@link AgeOffIterator} is appended to the input iterator to provide
     * age-off functionality.
     *
     * @param  source the input iterator
     * @return        the source iterator or a new filtering iterator
     */
    private CloseableIterator<Row> maybeCreateFilter(CloseableIterator<Row> source) {
        return config.ageOffColumn().map(filter_col -> {
            AgeOffIterator ageoff = new AgeOffIterator();
            // Age off iterator operates in milliseconds
            ageoff.init(String.format("%s,%d", filter_col, config.maxAge()), schema);
            return ageoff.applyTransform(source);
        }).orElse(source);
    }
}
