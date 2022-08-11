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
package sleeper.core.range;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;

public class RangeCanonicaliserTest {

    @Test
    public void shouldAnswerIsRangeInCanonicalFormCorrectly() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, 1, 10);
        Range range2 = rangeFactory.createRange(field, 1, true, 10, true);
        Range range3 = rangeFactory.createRange(field, 1, false, 10, true);
        Range range4 = rangeFactory.createRange(field, 1, false, 10, false);

        // When / Then
        assertThat(RangeCanonicaliser.isRangeInCanonicalForm(range1)).isTrue();
        assertThat(RangeCanonicaliser.isRangeInCanonicalForm(range2)).isFalse();
        assertThat(RangeCanonicaliser.isRangeInCanonicalForm(range3)).isFalse();
        assertThat(RangeCanonicaliser.isRangeInCanonicalForm(range4)).isFalse();
    }

    @Test
    public void shouldCanonicaliseRangeCorrectly() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, 1, 10);
        Range range2 = rangeFactory.createRange(field, 1, true, 10, true);
        Range range3 = rangeFactory.createRange(field, 1, false, 10, true);
        Range range4 = rangeFactory.createRange(field, 1, false, 10, false);

        // When
        Range canonicalisedRange1 = RangeCanonicaliser.canonicaliseRange(range1);
        Range canonicalisedRange2 = RangeCanonicaliser.canonicaliseRange(range2);
        Range canonicalisedRange3 = RangeCanonicaliser.canonicaliseRange(range3);
        Range canonicalisedRange4 = RangeCanonicaliser.canonicaliseRange(range4);

        // Then
        assertThat(canonicalisedRange1).isEqualTo(range1);
        assertThat(canonicalisedRange2).isEqualTo(rangeFactory.createRange(field, 1, 11));
        assertThat(canonicalisedRange3).isEqualTo(rangeFactory.createRange(field, 2, 11));
        assertThat(canonicalisedRange4).isEqualTo(rangeFactory.createRange(field, 2, 10));
    }
}
