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
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RangeFactoryTest {

    @Test
    public void shouldCreateCorrectRangesForIntKey() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Range range1 = rangeFactory.createRange(field, 1, true, 10, true);
        Range range2 = rangeFactory.createRange(field, 1, true, 10, false);
        Range range3 = rangeFactory.createRange(field, 1, false, 10, true);
        Range range4 = rangeFactory.createRange(field, 1, false, 10, false);
        Range range5 = rangeFactory.createRange(field, Integer.MIN_VALUE, true, null, true);
        Range range6 = rangeFactory.createExactRange(field, 1);

        // Then
        assertThat(range1.getMin()).isEqualTo(1);
        assertThat(range1.isMinInclusive()).isTrue();
        assertThat(range1.getMax()).isEqualTo(10);
        assertThat(range1.isMaxInclusive()).isTrue();
        assertThat(range2.getMin()).isEqualTo(1);
        assertThat(range2.isMinInclusive()).isTrue();
        assertThat(range2.getMax()).isEqualTo(10);
        assertThat(range2.isMaxInclusive()).isFalse();
        assertThat(range3.getMin()).isEqualTo(1);
        assertThat(range3.isMinInclusive()).isFalse();
        assertThat(range3.getMax()).isEqualTo(10);
        assertThat(range3.isMaxInclusive()).isTrue();
        assertThat(range4.getMin()).isEqualTo(1);
        assertThat(range4.isMinInclusive()).isFalse();
        assertThat(range4.getMax()).isEqualTo(10);
        assertThat(range4.isMinInclusive()).isFalse();
        assertThat(range5.getMin()).isEqualTo(Integer.MIN_VALUE);
        assertThat(range5.isMinInclusive()).isTrue(); // If max is null, maxInclusive should be set to false
        assertThat(range5.getMax()).isNull();
        assertThat(range4.isMaxInclusive()).isFalse();
        assertThat(range6.getMin()).isEqualTo(1);
        assertThat(range6.isMinInclusive()).isTrue();
        assertThat(range6.getMax()).isEqualTo(1);
        assertThat(range6.isMaxInclusive()).isTrue();
    }

    @Test
    public void shouldNotAcceptNullMin() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When / Then
        assertThatThrownBy(() -> rangeFactory.createRange(field, null, false, "A", true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
