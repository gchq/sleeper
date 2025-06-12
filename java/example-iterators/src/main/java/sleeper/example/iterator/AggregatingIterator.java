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
import sleeper.core.schema.Schema;

import java.util.List;

/**
 * Implements a record aggregating iterator similiar to the DataFusion aggregation functionality.
 *
 * The format for the iterator's configuration string mirrors that of the DataFusion code. It should follow this format:
 * {@code <extra aggregation columns>;<filter>,<aggregation>}. Any of the components may be empty. The extra aggregation
 * columns should be a comma separated list of columns beyond the row_key columns to aggregate over. Note the semi-colon
 * after the last column before the filter component. The filter component will expose one possible filter: age off.
 * Only one filter will be specifiable at a time on a table (this is a minimum reasonable effort!).
 * <ul>
 * <li>Age off filter format is `ageoff='column','time_period'. If the elapsed time from the integer value in the given
 * column to "now" is lower than the time_period value, the row is retained. Values in seconds.</li>
 * <li>Aggregation filter format is OP(column_name) where OP is one of sum, min, max, map_sum, map_min or map_max.</li>
 * </ul>
 *
 * We may decide to expand the list of allowable operations. Additional aggregation filters should be comma separated.
 */
public class AggregatingIterator implements SortedRecordIterator {

    @Override
    public CloseableIterator<Record> apply(CloseableIterator<Record> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'apply'");
    }

    /**
     * Configures the iterator.
     *
     * The configuration string will be validated and the internal state of the iterator will be initialised.
     *
     * @throws IllegalArgumentException if {@code configString} is not a valid configuration
     */
    @Override
    public void init(String configString, Schema schema) {
        parseConfiguration(configString);
    }

    /**
     * Parses a configuration string for aggregation.
     *
     * Validates that:
     * <ol>
     * <li>All columns that are NOT query aggregation columns have an aggregation operation specified for them.</li>
     * <li>No query aggregation columns have aggregations specified.</li>
     * <li>No query aggregation column is duplicated.</li>
     * <li>Aggregation columns must be valid in schema.</li>
     * </ol>
     *
     * @implNote                          This is a minimum viable parser for the configuration for filters/aggregators.
     *
     * @throws   IllegalArgumentException if {@code configString} is invalid
     */
    public static void parseConfiguration(String configString) {

    }

    @Override
    public List<String> getRequiredValueFields() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRequiredValueFields'");
    }
}
