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
package sleeper.spark;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.type.StringType;
import sleeper.spark.SplitPushedFiltersIntoSingleAndMultiRegionFilters.SingleAndMultiRegionFilters;

import static org.assertj.core.api.Assertions.assertThat;

public class SplitPushedFiltersIntoSingleAndMultiRegionFiltersTest {
    private static final Field ROW_KEY_FIELD = new Field("key", new StringType());

    @Test
    void shouldSplitFiltersWhenOneEqualOnRowKeyField() {
        // Given
        SplitPushedFiltersIntoSingleAndMultiRegionFilters split = new SplitPushedFiltersIntoSingleAndMultiRegionFilters();
        EqualTo equalTo = new EqualTo(ROW_KEY_FIELD.getName(), "G");
        Filter[] filters = new Filter[]{equalTo};

        // When
        SingleAndMultiRegionFilters singleAndMultiRegionFilters = split.splitPushedFilters(filters);

        // Then
        assertThat(singleAndMultiRegionFilters.getSingleRegionFilters()).containsExactly(equalTo);
        assertThat(singleAndMultiRegionFilters.getMultiRegionFilters()).isEmpty();
    }

    @Test
    void shouldSplitFiltersWhenOneIsInOnRowKeyField() {
        // Given
        SplitPushedFiltersIntoSingleAndMultiRegionFilters split = new SplitPushedFiltersIntoSingleAndMultiRegionFilters();
        In in = new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B", "C"});
        Filter[] filters = new Filter[]{in};

        // When
        SingleAndMultiRegionFilters singleAndMultiRegionFilters = split.splitPushedFilters(filters);

        // Then
        assertThat(singleAndMultiRegionFilters.getSingleRegionFilters()).isEmpty();
        assertThat(singleAndMultiRegionFilters.getMultiRegionFilters()).containsExactly(in);
    }
}
