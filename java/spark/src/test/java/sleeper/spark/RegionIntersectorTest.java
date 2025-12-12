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

import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.RangeCanonicaliser;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class RegionIntersectorTest {
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
    void shouldIntersectEmptyListOfRegionsToGiveNoRegion() {
        // Given
        List<Region> regions = List.of();

        // When
        Optional<Region> optionalRegion = RegionIntersector.intersectRegions(regions, RANGE_FACTORY, SCHEMA);

        // Then
        assertThat(optionalRegion).isEmpty();
    }

    @Test
    void shouldIntersectSingleRegionToGiveSameRegion() {
        // Given
        Region region = new Region(RANGE_FACTORY.createRange(ROW_KEY_FIELD, "E", false, "T", false));
        List<Region> regions = List.of(region);

        // When
        Optional<Region> optionalRegion = RegionIntersector.intersectRegions(regions, RANGE_FACTORY, SCHEMA);

        // Then
        assertThat(optionalRegion).contains(region);
    }

    // Currently fails because Range equality works on the non-canonicalised form.
    @Test
    void shouldIntersectTwoRegionsOnDifferentRowKeys() {
        // Given
        Region region1 = new Region(List.of(RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD.getName(), "E"),
                RANGE_FACTORY2.createRangeCoveringAllValues(ROW_KEY_FIELD2)));
        Region region2 = new Region(List.of(RANGE_FACTORY2.createRangeCoveringAllValues(ROW_KEY_FIELD),
                RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2.getName(), "T")));
        List<Region> regions = List.of(region1, region2);

        // When
        Optional<Region> optionalRegion = RegionIntersector.intersectRegions(regions, RANGE_FACTORY2, SCHEMA2);

        // Then
        Region expectedRegion = new Region(List.of(RangeCanonicaliser.canonicaliseRange(RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD.getName(), "E")),
                RangeCanonicaliser.canonicaliseRange(RANGE_FACTORY2.createExactRange(ROW_KEY_FIELD2.getName(), "T"))));
        assertThat(optionalRegion).contains(expectedRegion);
    }

}
