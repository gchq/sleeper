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
package sleeper.core.range.canonicaliser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.RangeCanonicaliser;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeCanonicaliserLongTypeTest {
    private Field field;
    private RangeFactory rangeFactory;

    @BeforeEach
    public void setup() {
        field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        rangeFactory = new RangeFactory(schema);
    }

    @Test
    public void shouldCanonicaliseRangeMinInclusiveMaxExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1L, 10L);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    @Test
    public void shouldCanonicaliseRangeMinInclusiveMaxInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1L, true, 10L, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, 1L, 11L));
    }

    @Test
    public void shouldCanonicaliseRangeMinExclusiveMaxInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1L, false, 10L, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, 2L, 11L));
    }

    @Test
    public void shouldCanonicaliseRangeMinExclusiveMaxExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1L, false, 10L, false);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, 2L, 10L));
    }

    @Test
    public void shouldAlreadyBeCanonicalWhenMaxIsNullExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1L, true, null, false);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    @Test
    public void shouldSetRangeToMaxExclusiveWhenSpecifiedAsNullInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1L, true, null, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then;
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }
}
