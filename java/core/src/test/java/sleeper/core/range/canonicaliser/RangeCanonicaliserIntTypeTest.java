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
import sleeper.core.schema.type.IntType;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeCanonicaliserIntTypeTest {
    private Field field;
    private RangeFactory rangeFactory;

    @BeforeEach
    public void setup() {
        field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        rangeFactory = new RangeFactory(schema);
    }

    @Test
    public void shouldCanonicaliseRangeMinInclusiveMaxExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1, 10);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    @Test
    public void shouldCanonicaliseRangeMinInclusiveMaxInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1, true, 10, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, 1, 11));
    }

    @Test
    public void shouldCanonicaliseRangeMinExclusiveMaxInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1, false, 10, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, 2, 11));
    }

    @Test
    public void shouldCanonicaliseRangeMinExclusiveMaxExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1, false, 10, false);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, 2, 10));
    }

    @Test
    public void shouldAlreadyBeCanonicalWhenMaxIsNullExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1, true, null, false);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    @Test
    public void shouldSetRangeToMaxExclusiveWhenSpecifiedAsNullInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, 1, true, null, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then;
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }
}
