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

import sleeper.core.iterator.AggregationFilteringIterator.Aggregation;
import sleeper.core.iterator.AggregationFilteringIterator.FilterAggregationConfig;
import sleeper.core.key.Key;
import sleeper.core.record.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Performs aggregation of record columns based on specified column names and aggregation operators.
 */
public class AggregatorIteratorImpl implements CloseableIterator<sleeper.core.record.Record> {

    /** Which columns to group and aggregate. */
    private final FilterAggregationConfig config;
    /** Source iterator. */
    private final CloseableIterator<sleeper.core.record.Record> input;
    /**
     * The record retrieved from the record source that is the start of the next aggregation group. If this is null,
     * then either we are at the start of the iteration (no input records retrieved yet), or the input iterator
     * has run out of records.
     */
    private Record startOfNextAggregationGroup = null;

    /**
     * Sets up a aggregating iterator.
     *
     * @param config the configuration
     */
    public AggregatorIteratorImpl(FilterAggregationConfig config, CloseableIterator<sleeper.core.record.Record> input) {
        this.config = Objects.requireNonNull(config, "config");
        this.input = Objects.requireNonNull(input, "input");
    }

    @Override
    public boolean hasNext() {
        // Is there a record left over from previous aggregation group or (at least) one more in the input source?
        return startOfNextAggregationGroup != null || input.hasNext();
    }

    @Override
    public void close() throws IOException {
        input.close();
        startOfNextAggregationGroup = null;
    }

    @Override
    public sleeper.core.record.Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        // There must be at least one more record, either stashed by us or in the input iterator
        Record aggregated = (startOfNextAggregationGroup != null) ? startOfNextAggregationGroup : input.next();
        // We may have just re-assigned the startOfNextAggregation group, so null it out
        startOfNextAggregationGroup = null;
        // Now aggregate more records on to this one until we find an unequal one or run out of data
        while (startOfNextAggregationGroup == null && input.hasNext()) {
            Record next = input.next();
            if (recordsEqual(aggregated, next)) {
                aggregateOnTo(aggregated, next, config);
            } else {
                startOfNextAggregationGroup = next;
            }
        }
        return aggregated;
    }

    /**
     * Aggregates all the aggregation fields from one record on to another.
     *
     * @param aggregated     the record to be modified
     * @param toBeAggregated the record containing new values
     * @param config         the aggregation configuration
     */
    public static void aggregateOnTo(Record aggregated, Record toBeAggregated, FilterAggregationConfig config) {
        for (Aggregation agg : config.aggregations()) {
            // Extract current and new value
            Object currentValue = aggregated.get(agg.column());
            Object newValue = toBeAggregated.get(agg.column());
            aggregated.put(agg.column(), agg.op().apply(currentValue, newValue));
        }
    }

    /**
     * Determines if two records are equal.
     *
     * @param  lhs record
     * @param  rhs record
     * @return     true if they are considered equal
     */
    public boolean recordsEqual(Record lhs, Record rhs) {
        List<Object> keys1 = new ArrayList<>();
        List<Object> keys2 = new ArrayList<>();
        for (String key : config.groupingColumns()) {
            keys1.add(lhs.get(key));
            keys2.add(rhs.get(key));
        }
        return Key.create(keys1).equals(Key.create(keys2));

    }
}
