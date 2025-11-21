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
package sleeper.datasource;

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateRegionsFromPushedFiltersTest {
    private static final Field ROW_KEY_FIELD = new Field("key", new StringType());
    private static final Field ROW_KEY_FIELD2 = new Field("key2", new StringType());
    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(ROW_KEY_FIELD)
            .valueFields(new Field("value", new StringType()))
            .build();
    private static final Schema SCHEMA2 = Schema.builder()
            .rowKeyFields(ROW_KEY_FIELD, ROW_KEY_FIELD2)
            .valueFields(new Field("value", new StringType()))
            .build();
    private static final RangeFactory RANGE_FACTORY = new RangeFactory(SCHEMA);
    private static final RangeFactory RANGE_FACTORY2 = new RangeFactory(SCHEMA2);

    @Test
    void shouldReturnEntireKeySpaceWhenNoFilters() {
        // Given
        Filter[] pushedFilters = new Filter[]{};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA, pushedFilters);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters();

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRangeCoveringAllValues(ROW_KEY_FIELD));
        assertThat(regions).containsExactly(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenEqualToFilterPushed() {
        // Given
        EqualTo equalTo = new EqualTo(ROW_KEY_FIELD.getName(), "E");
        Filter[] pushedFilters = new Filter[]{equalTo};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA, pushedFilters);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters();

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "E"));
        assertThat(regions).containsExactly(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenGreaterThanFilterPushed() {
        // Given
        GreaterThan greaterThan = new GreaterThan(ROW_KEY_FIELD.getName(), "E");
        Filter[] pushedFilters = new Filter[]{greaterThan};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA, pushedFilters);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters();

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, null, false));
        assertThat(regions).containsExactly(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenGreaterThanAndLessThanFiltersPushed() {
        // Given
        GreaterThan greaterThan = new GreaterThan(ROW_KEY_FIELD.getName(), "E");
        LessThan lessThan = new LessThan(ROW_KEY_FIELD.getName(), "Z");
        Filter[] pushedFilters = new Filter[]{greaterThan, lessThan};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA, pushedFilters);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters();

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, "Z", false));
        assertThat(regions).containsExactly(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenInFilterIsPushed() {
        // Given
        Object[] wantedKeys = new Object[]{"A", "C", "G", "U"};
        In in = new In(ROW_KEY_FIELD.getName(), wantedKeys);
        Filter[] pushedFilters = new Filter[]{in};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA, pushedFilters);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters();

        // Then
        Region expectedRegionA = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "A"));
        Region expectedRegionC = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "C"));
        Region expectedRegionG = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "G"));
        Region expectedRegionU = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "U"));
        assertThat(regions).containsExactlyInAnyOrder(expectedRegionA, expectedRegionC, expectedRegionG, expectedRegionU);
    }
}