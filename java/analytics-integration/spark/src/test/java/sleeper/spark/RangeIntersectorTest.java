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

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.RangeCanonicaliser;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeIntersectorTest {
    private static final Field ROW_KEY_FIELD = new Field("key", new StringType());
    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(ROW_KEY_FIELD)
            .valueFields(new Field("value", new StringType()))
            .build();
    private static final RangeFactory RANGE_FACTORY = new RangeFactory(SCHEMA);

    @Test
    void shouldIntersectRangeWithExactRangeToGiveExactRange() {
        // Given
        Range range1 = RANGE_FACTORY.createExactRange(ROW_KEY_FIELD.getName(), "E");
        Range range2 = RANGE_FACTORY.createRangeCoveringAllValues(ROW_KEY_FIELD);

        // When
        Optional<Range> optionalRange = RangeIntersector.intersectRanges(range1, range2, RANGE_FACTORY);

        // Then
        assertThat(optionalRange).contains(RangeCanonicaliser.canonicaliseRange(range1));
    }

    @Test
    void shouldIntersectRangeWithFullRangeToGiveRange() {
        // Given
        Range range1 = RANGE_FACTORY.createRange(ROW_KEY_FIELD.getName(), "E", true, "T", true);
        Range range2 = RANGE_FACTORY.createRangeCoveringAllValues(ROW_KEY_FIELD);

        // When
        Optional<Range> optionalRange = RangeIntersector.intersectRanges(range1, range2, RANGE_FACTORY);

        // Then
        assertThat(optionalRange).contains(RangeCanonicaliser.canonicaliseRange(range1));
    }

    @Test
    void shouldIntersectTwoOverlappingRangesCorrectly() {
        // Given
        Range range1 = RANGE_FACTORY.createRange(ROW_KEY_FIELD.getName(), "E", true, "T", true);
        Range range2 = RANGE_FACTORY.createRange(ROW_KEY_FIELD.getName(), "F", true, "Z", true);

        // When
        Optional<Range> optionalRange = RangeIntersector.intersectRanges(range1, range2, RANGE_FACTORY);

        // Then
        Range expectedRange = RANGE_FACTORY.createRange(ROW_KEY_FIELD.getName(), "F", true, "T", true);
        assertThat(optionalRange).contains(RangeCanonicaliser.canonicaliseRange(expectedRange));
    }

    @Test
    void shouldIntersectTwoNonOverlappingRangesToGiveNoRange() {
        // Given
        Range range1 = RANGE_FACTORY.createRange(ROW_KEY_FIELD.getName(), "E", true, "F", true);
        Range range2 = RANGE_FACTORY.createRange(ROW_KEY_FIELD.getName(), "G", true, "Z", true);

        // When
        Optional<Range> optionalRange = RangeIntersector.intersectRanges(range1, range2, RANGE_FACTORY);

        // Then
        assertThat(optionalRange).isEmpty();
    }
}
