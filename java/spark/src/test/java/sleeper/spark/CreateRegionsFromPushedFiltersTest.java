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
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.range.RegionCanonicaliser;
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
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(new Region(RANGE_FACTORY.createRangeCoveringAllValues(ROW_KEY_FIELD)));
        assertThat(regions).hasSize(1);
        assertThat(RegionCanonicaliser.canonicaliseRegion(regions.get(0))).isEqualTo(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenEqualToFilterPushed() {
        // Given
        EqualTo equalTo = new EqualTo(ROW_KEY_FIELD.getName(), "E");
        Filter[] pushedFilters = new Filter[]{equalTo};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "E")));
        assertThat(regions).hasSize(1);
        assertThat(RegionCanonicaliser.canonicaliseRegion(regions.get(0))).isEqualTo(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenGreaterThanAndEqualFiltersPushed() {
        // Given
        GreaterThan greaterThanRowKey1 = new GreaterThan(ROW_KEY_FIELD.getName(), "E");
        EqualTo equalToRowKey2 = new EqualTo(ROW_KEY_FIELD2.getName(), "J");
        Filter[] pushedFilters = new Filter[]{greaterThanRowKey1, equalToRowKey2};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA2);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(new Region(List.of(
                RANGE_FACTORY2.createRange(ROW_KEY_FIELD, "E", false, null, false),
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2, "J"))));
        assertThat(regions).hasSize(1);
        assertThat(RegionCanonicaliser.canonicaliseRegion(regions.get(0))).isEqualTo(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenGreaterThanFilterPushed() {
        // Given
        GreaterThan greaterThan = new GreaterThan(ROW_KEY_FIELD.getName(), "E");
        Filter[] pushedFilters = new Filter[]{greaterThan};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(
                new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, null, false)));
        assertThat(regions).hasSize(1);
        assertThat(RegionCanonicaliser.canonicaliseRegion(regions.get(0))).isEqualTo(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenGreaterThanAndLessThanFiltersPushed() {
        // Given
        GreaterThan greaterThan = new GreaterThan(ROW_KEY_FIELD.getName(), "E");
        LessThan lessThan = new LessThan(ROW_KEY_FIELD.getName(), "Z");
        Filter[] pushedFilters = new Filter[]{greaterThan, lessThan};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(
                new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, "Z", false)));
        assertThat(regions).hasSize(1);
        assertThat(RegionCanonicaliser.canonicaliseRegion(regions.get(0))).isEqualTo(expectedRegion);
    }

    @Test
    void shouldReturnCorrectRegionWhenInFilterIsPushed() {
        // Given
        Object[] wantedKeys = new Object[]{"A", "C", "G", "U"};
        In in = new In(ROW_KEY_FIELD.getName(), wantedKeys);
        Filter[] pushedFilters = new Filter[]{in};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegionA = RegionCanonicaliser.canonicaliseRegion(new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "A")));
        Region expectedRegionC = RegionCanonicaliser.canonicaliseRegion(new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "C")));
        Region expectedRegionG = RegionCanonicaliser.canonicaliseRegion(new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "G")));
        Region expectedRegionU = RegionCanonicaliser.canonicaliseRegion(new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "U")));
        assertThat(regions).containsExactlyInAnyOrder(expectedRegionA, expectedRegionC, expectedRegionG, expectedRegionU);
    }

    @Test
    void shouldReturnCorrectRegionWhenEqualToFilterPushedWith2DKey() {
        // Given
        EqualTo equalTo = new EqualTo(ROW_KEY_FIELD.getName(), "E");
        EqualTo equalTo2 = new EqualTo(ROW_KEY_FIELD2.getName(), "T");
        Filter[] pushedFilters = new Filter[]{equalTo, equalTo2};
        CreateRegionsFromPushedFilters createRegionsFromPushedFilters = new CreateRegionsFromPushedFilters(SCHEMA2);

        // When
        List<Region> regions = createRegionsFromPushedFilters.getMinimumRegionCoveringPushedFilters(pushedFilters);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(new Region(
                List.of(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "E"), RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2, "T"))));
        assertThat(regions).hasSize(1);
        assertThat(RegionCanonicaliser.canonicaliseRegion(regions.get(0))).isEqualTo(expectedRegion);
    }
}
