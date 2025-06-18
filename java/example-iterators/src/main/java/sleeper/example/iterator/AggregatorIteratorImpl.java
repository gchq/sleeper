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
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.example.iterator.AggregationFilteringIterator.Aggregation;
import sleeper.example.iterator.AggregationFilteringIterator.FilterAggregationConfig;

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
    /** The record to be returned next. */
    private Record next;
    /** The record that starts the next aggregation group. */
    private Record toBeAggregated;

    /**
     * Sets up a aggregating iterator.
     *
     * @param config the configuration
     */
    public AggregatorIteratorImpl(FilterAggregationConfig config, CloseableIterator<sleeper.core.record.Record> input) {
        this.config = Objects.requireNonNull(config, "config");
        this.input = Objects.requireNonNull(input, "input");
        this.next = getNextRecord();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public void close() throws IOException {
        input.close();
        next = null;
        toBeAggregated = null;
    }

    @Override
    public sleeper.core.record.Record next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        Record toReturn = new Record(next);
        // Update next record, this will become null if no more data
        next = getNextRecord();
        return toReturn;
    }

    /**
     * Aggregates from the input iterator to create the next fully aggregated record.
     *
     * @return the next aggregated record
     */
    protected Record getNextRecord() {
        // If this is the first run, then both record fields might be null
        if (toBeAggregated == null && input.hasNext()) {
            toBeAggregated = input.next();
        }
        // Assertion if toBeAggregated is null, then there is nothing more we can do
        if (toBeAggregated != null) {
            // We will now aggregate "on to" this record
            Record aggregated = toBeAggregated;
            toBeAggregated = null;
            // Aggregation loop
            while (input.hasNext()) {
                toBeAggregated = input.next();
                if (recordsEqual(aggregated, toBeAggregated)) {
                    // Do aggregation of spare into aggregated
                    aggregateOnTo(aggregated, toBeAggregated, config);
                }
            }
            return aggregated;
        } else {
            return null;
        }
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
