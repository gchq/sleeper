/*
 * Copyright 2022-2026 Crown Copyright
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

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.range.RegionCanonicaliser;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CreateRegionFromFilterTest {
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
    void shouldCreateCorrectRegionFromEqualToFilter() {
        // Given
        EqualTo equalTo = new EqualTo(ROW_KEY_FIELD.getName(), "E");

        // When
        Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(equalTo, SCHEMA);

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "E"));
        assertThat(region).isEqualTo(expectedRegion);
    }

    @Test
    void shouldCreateCorrectRegionFromGreaterThanFilter() {
        // Given
        GreaterThan greaterThan = new GreaterThan(ROW_KEY_FIELD.getName(), "E");

        // When
        Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(greaterThan, SCHEMA);

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, null, false));
        assertThat(region).isEqualTo(expectedRegion);
    }

    @Test
    void shouldCreateCorrectRegionFromGreaterThanOrEqualFilter() {
        // Given
        GreaterThanOrEqual greaterThan = new GreaterThanOrEqual(ROW_KEY_FIELD.getName(), "E");

        // When
        Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(greaterThan, SCHEMA);

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", true, null, false));
        assertThat(region).isEqualTo(expectedRegion);
    }

    @Test
    void shouldCreateCorrectRegionFromLessThanFilter() {
        // Given
        LessThan lessThan = new LessThan(ROW_KEY_FIELD.getName(), "E");

        // When
        Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(lessThan, SCHEMA);

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "", true, "E", false));
        assertThat(region).isEqualTo(expectedRegion);
    }

    @Test
    void shouldCreateCorrectRegionFromLessThanOrEqualFilter() {
        // Given
        LessThanOrEqual lessThanOrEqual = new LessThanOrEqual(ROW_KEY_FIELD.getName(), "E");

        // When
        Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(lessThanOrEqual, SCHEMA);

        // Then
        Region expectedRegion = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "", true, "E", true));
        assertThat(region).isEqualTo(expectedRegion);
    }

    @Test
    void shouldCreateCorrectRegionsFromOrFilter() {
        // Given
        Or or = new Or(
                new LessThan(ROW_KEY_FIELD.getName(), "B"),
                new GreaterThan(ROW_KEY_FIELD.getName(), "Y"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(or, SCHEMA);

        // Then
        Region expectedRegionBelowB = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "", true, "B", false));
        Region expectedRegionAboveY = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "Y", false, null, false));
        assertThat(regions).containsExactlyInAnyOrder(expectedRegionBelowB, expectedRegionAboveY);
    }

    @Test
    void shouldCreateCorrectRegionsFromOrFilterOnTwoRowKeys() {
        // Given
        Or or = new Or(
                new EqualTo(ROW_KEY_FIELD.getName(), "A"),
                new EqualTo(ROW_KEY_FIELD2.getName(), "B"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(or, SCHEMA2);

        // Then each region should leave the other row key unbounded
        Region expectedRegionKeyA = new Region(List.of(
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD, "A"),
                RANGE_FACTORY2.createRangeCoveringAllValues(ROW_KEY_FIELD2)));
        Region expectedRegionKey2B = new Region(List.of(
                RANGE_FACTORY2.createRangeCoveringAllValues(ROW_KEY_FIELD),
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2, "B")));
        assertThat(regions).containsExactlyInAnyOrder(expectedRegionKeyA, expectedRegionKey2B);
    }

    @Test
    void shouldIgnoreEmptyInFilterWithinOrFilter() {
        // Given
        Or or = new Or(
                new In(ROW_KEY_FIELD.getName(), new Object[]{}),
                new EqualTo(ROW_KEY_FIELD.getName(), "C"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(or, SCHEMA);

        // Then
        Region expectedRegionC = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "C"));
        assertThat(regions).containsExactly(expectedRegionC);
    }

    @Test
    void shouldCreateCorrectRegionsFromNestedOrFilter() {
        // Given
        // (key = "A" OR key = "B") OR key = "C"
        Or or = new Or(
                new Or(new EqualTo(ROW_KEY_FIELD.getName(), "A"), new EqualTo(ROW_KEY_FIELD.getName(), "B")),
                new EqualTo(ROW_KEY_FIELD.getName(), "C"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(or, SCHEMA);

        // Then
        Region expectedRegionA = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "A"));
        Region expectedRegionB = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "B"));
        Region expectedRegionC = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "C"));
        assertThat(regions).containsExactlyInAnyOrder(expectedRegionA, expectedRegionB, expectedRegionC);
    }

    @Test
    void shouldCreateCorrectRegionsFromOrOfInFilter() {
        // Given
        Or or = new Or(
                new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B"}),
                new EqualTo(ROW_KEY_FIELD.getName(), "C"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(or, SCHEMA);

        // Then
        Region expectedRegionA = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "A"));
        Region expectedRegionB = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "B"));
        Region expectedRegionC = new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "C"));
        assertThat(regions).containsExactlyInAnyOrder(expectedRegionA, expectedRegionB, expectedRegionC);
    }

    @Test
    void shouldCreateCorrectRegionFromAndFilter() {
        // Given
        And and = new And(
                new GreaterThan(ROW_KEY_FIELD.getName(), "E"),
                new LessThan(ROW_KEY_FIELD.getName(), "Z"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(and, SCHEMA);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(
                new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, "Z", false)));
        assertThat(regions.stream().map(RegionCanonicaliser::canonicaliseRegion))
                .containsExactly(expectedRegion);
    }

    @Test
    void shouldCreateCorrectRegionsFromOrFilterWithAndBranch() {
        // Given
        // (key > "A" AND key < "C") OR key = "G"
        Or or = new Or(
                new And(new GreaterThan(ROW_KEY_FIELD.getName(), "A"), new LessThan(ROW_KEY_FIELD.getName(), "C")),
                new EqualTo(ROW_KEY_FIELD.getName(), "G"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(or, SCHEMA);

        // Then
        Region expectedRegionAToC = RegionCanonicaliser.canonicaliseRegion(
                new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "A", false, "C", false)));
        Region expectedRegionG = RegionCanonicaliser.canonicaliseRegion(
                new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "G")));
        assertThat(regions.stream().map(RegionCanonicaliser::canonicaliseRegion))
                .containsExactlyInAnyOrder(expectedRegionAToC, expectedRegionG);
    }

    @Test
    void shouldCreateOverlapOnlyFromAndOfInFilters() {
        // Given
        And and = new And(
                new In(ROW_KEY_FIELD.getName(), new Object[]{"A", "B"}),
                new In(ROW_KEY_FIELD.getName(), new Object[]{"B", "C"}));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(and, SCHEMA);

        // Then only the overlapping key should remain, the empty intersections should be dropped
        Region expectedRegionB = RegionCanonicaliser.canonicaliseRegion(
                new Region(RANGE_FACTORY.createExactRange(ROW_KEY_FIELD, "B")));
        assertThat(regions.stream().map(RegionCanonicaliser::canonicaliseRegion))
                .containsExactly(expectedRegionB);
    }

    @Test
    void shouldCreateCorrectRegionFromAndFilterOnTwoRowKeys() {
        // Given
        And and = new And(
                new EqualTo(ROW_KEY_FIELD.getName(), "A"),
                new EqualTo(ROW_KEY_FIELD2.getName(), "B"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(and, SCHEMA2);

        // Then
        Region expectedRegion = RegionCanonicaliser.canonicaliseRegion(new Region(List.of(
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD, "A"),
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2, "B"))));
        assertThat(regions.stream().map(RegionCanonicaliser::canonicaliseRegion))
                .containsExactly(expectedRegion);
    }

    @Test
    void shouldCreateNoRegionsFromAndFilterWithContradictingBranches() {
        // Given
        And and = new And(
                new EqualTo(ROW_KEY_FIELD.getName(), "A"),
                new EqualTo(ROW_KEY_FIELD.getName(), "B"));

        // When
        List<Region> regions = CreateRegionFromFilter.createRegionsFromFilter(and, SCHEMA);

        // Then
        assertThat(regions).isEmpty();
    }

    @Test
    void shouldCreateCorrectRegionFromEqualToFilterOn2ndRowKey() {
        // Given
        EqualTo equalTo = new EqualTo(ROW_KEY_FIELD2.getName(), "E");

        // When
        Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(equalTo, SCHEMA2);

        // Then
        Region expectedRegion = new Region(List.of(RANGE_FACTORY.createRangeCoveringAllValues(ROW_KEY_FIELD),
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2, "E")));
        assertThat(region).isEqualTo(expectedRegion);
    }
}
