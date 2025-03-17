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
package sleeper.core.range;

import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class RegionCanonicaliserTest {

    @Test
    public void shouldAnswerIsRegionInCanonicalFormCorrectly() {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, 1, 10);
        Range range2 = rangeFactory.createRange(field2, 100L, true, 200L, true);
        Region region1 = new Region(Arrays.asList(range1, range2));
        Range range3 = rangeFactory.createRange(field1, 1, 10);
        Range range4 = rangeFactory.createRange(field2, 100L, 1000L);
        Region region2 = new Region(Arrays.asList(range3, range4));

        // When / Then
        assertThat(RegionCanonicaliser.isRegionInCanonicalForm(region1)).isFalse();
        assertThat(RegionCanonicaliser.isRegionInCanonicalForm(region2)).isTrue();
    }

    @Test
    public void shouldCanonicaliseRegionCorrectly() {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, 1, 10);
        Range range2 = rangeFactory.createRange(field2, 100L, true, 200L, true);
        Region region1 = new Region(Arrays.asList(range1, range2));
        Range range3 = rangeFactory.createRange(field1, 1, 10);
        Range range4 = rangeFactory.createRange(field2, 100L, 1000L);
        Region region2 = new Region(Arrays.asList(range3, range4));

        // When
        Region canonicalisedRegion1 = RegionCanonicaliser.canonicaliseRegion(region1);
        Region canonicalisedRegion2 = RegionCanonicaliser.canonicaliseRegion(region2);

        // Then
        assertThat(canonicalisedRegion1.getRange("key1")).isEqualTo(range1);
        assertThat(canonicalisedRegion1.getRange("key2")).isEqualTo(rangeFactory.createRange(field2, 100L, 201L));
        assertThat(canonicalisedRegion2.getRange("key1")).isEqualTo(range3);
        assertThat(canonicalisedRegion2.getRange("key2")).isEqualTo(range4);
    }
}
