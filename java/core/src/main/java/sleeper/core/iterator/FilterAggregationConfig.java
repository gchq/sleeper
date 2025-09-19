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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Defines the filtering and aggregation configuration for this iterator.
 *
 * @param  groupingColumns           the columns that the aggregation will "group by"
 * @param  ageOffColumn              the optional column to have an age-off filter applied
 * @param  maxAge                    the maximum age in seconds for rows if an age off filter is to be applied
 * @param  aggregations              the list of aggregations to apply
 * @throws IllegalArgumentExpception if {@code groupingColumns} is empty
 */
public record FilterAggregationConfig(List<String> groupingColumns, Optional<String> ageOffColumn, long maxAge, List<Aggregation> aggregations) {

    public FilterAggregationConfig {
        Objects.requireNonNull(groupingColumns, "groupingColumns");
        if (groupingColumns.isEmpty()) {
            throw new IllegalArgumentException("must have at least one grouping column");
        }
        Objects.requireNonNull(ageOffColumn, "ageOffColumn");
        Objects.requireNonNull(aggregations, "aggregations");
    }

    /**
     * Retrieves the filtering configuration if set.
     *
     * @return the configuration
     */
    public Optional<AgeOffFilter> ageOffFilter() {
        return ageOffColumn.map(column -> new AgeOffFilter(column, maxAge));
    }
}
