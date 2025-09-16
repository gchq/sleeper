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
import sleeper.core.row.RowComparator;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Performs aggregation of columns based on specified column names and aggregation operators.
 */
public class AggregatorIteratorImpl implements CloseableIterator<Row> {

    /** Which columns to group and aggregate. */
    private final FilterAggregationConfig config;
    /** Source iterator. */
    private final CloseableIterator<Row> input;

    private final Schema schema;

    /**
     * The row retrieved from the row source that is the start of the next aggregation group. If this is null,
     * then either we are at the start of the iteration (no input rows retrieved yet), or the input iterator
     * has run out of rows.
     */
    private Row startOfNextAggregationGroup = null;

    /**
     * Sets up a aggregating iterator.
     *
     * @param input  the source iterator
     * @param config the configuration
     * @param schema the table schema
     */
    AggregatorIteratorImpl(FilterAggregationConfig config, CloseableIterator<Row> input, Schema schema) {
        this.config = Objects.requireNonNull(config, "config");
        this.input = Objects.requireNonNull(input, "input");
        this.schema = Objects.requireNonNull(schema, "schema");
    }

    @Override
    public boolean hasNext() {
        // Is there a row left over from previous aggregation group or (at least) one more in the input source?
        return startOfNextAggregationGroup != null || input.hasNext();
    }

    @Override
    public void close() throws IOException {
        input.close();
        startOfNextAggregationGroup = null;
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        RowComparator rowCompare = new RowComparator(schema);
        // There must be at least one more row, either stashed by us or in the input iterator
        Row aggregated = (startOfNextAggregationGroup != null) ? startOfNextAggregationGroup : input.next();
        // We may have just re-assigned the startOfNextAggregation group, so null it out
        startOfNextAggregationGroup = null;
        // Now aggregate more rows on to this one until we find an unequal one or run out of data
        while (startOfNextAggregationGroup == null && input.hasNext()) {
            Row next = input.next();
            if (rowCompare.compare(aggregated, next) == 0) {
                aggregateOnTo(aggregated, next, config);
            } else {
                startOfNextAggregationGroup = next;
            }
        }
        return aggregated;
    }

    /**
     * Aggregates all the aggregation fields from one row on to another.
     *
     * @param aggregated     the row to be modified
     * @param toBeAggregated the row containing new values
     * @param config         the aggregation configuration
     */
    public static void aggregateOnTo(Row aggregated, Row toBeAggregated, FilterAggregationConfig config) {
        for (Aggregation agg : config.aggregations()) {
            // Extract current and new value
            Object currentValue = aggregated.get(agg.column());
            Object newValue = toBeAggregated.get(agg.column());
            aggregated.put(agg.column(), agg.op().apply(currentValue, newValue));
        }
    }
}
