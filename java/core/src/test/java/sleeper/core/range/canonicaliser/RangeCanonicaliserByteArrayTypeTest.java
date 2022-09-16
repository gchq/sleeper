/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Before;
import org.junit.Test;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.RangeCanonicaliser;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeCanonicaliserByteArrayTypeTest {
    private Field field;
    private RangeFactory rangeFactory;

    @Before
    public void setup() {
        field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        rangeFactory = new RangeFactory(schema);
    }

    @Test
    public void shouldCanonicaliseRangeMinInclusiveMaxExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, new byte[]{1}, new byte[]{10});

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    @Test
    public void shouldCanonicaliseRangeMinInclusiveMaxInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, new byte[]{1}, true, new byte[]{10}, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, new byte[]{1}, nextByteArrayValue(new byte[]{10})));
    }

    @Test
    public void shouldCanonicaliseRangeMinExclusiveMaxInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, new byte[]{1}, false, new byte[]{10}, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, nextByteArrayValue(new byte[]{1}), nextByteArrayValue(new byte[]{10})));
    }

    @Test
    public void shouldCanonicaliseRangeMinExclusiveMaxExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, new byte[]{1}, false, new byte[]{10}, false);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isFalse();
        assertThat(canonicalisedRange).isEqualTo(rangeFactory.createRange(field, nextByteArrayValue(new byte[]{1}), new byte[]{10}));
    }

    @Test
    public void shouldHandleRangeWithMaxNullExclusive() {
        //Given
        Range range = rangeFactory.createRange(field, new byte[]{1}, true, null, false);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    @Test
    public void shouldHandleRangeWithMaxNullInclusive() {
        //Given
        Range range = rangeFactory.createRange(field, new byte[]{1}, true, null, true);

        //When
        Range canonicalisedRange = RangeCanonicaliser.canonicaliseRange(range);

        //Then;
        assertThat(range.isInCanonicalForm()).isTrue();
        assertThat(canonicalisedRange).isEqualTo(range);
    }

    private byte[] nextByteArrayValue(byte[] value) {
        byte[] next = new byte[value.length + 1];
        System.arraycopy(value, 0, next, 0, value.length);
        next[value.length] = Byte.MIN_VALUE;
        return next;
    }
}
