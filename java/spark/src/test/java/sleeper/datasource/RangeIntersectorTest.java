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
package sleeper.datasource;

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
    void should() {
        // Given
        Range range1 = RANGE_FACTORY.createExactRange(ROW_KEY_FIELD.getName(), "E");
        Range range2 = RANGE_FACTORY.createRangeCoveringAllValues(ROW_KEY_FIELD);

        String x = (String) RangeCanonicaliser.canonicaliseRange(range1).getMax();
        String y = (String) RangeCanonicaliser.canonicaliseRange(range1).getMin();
        System.out.println("Canonicalised range to " + RangeCanonicaliser.canonicaliseRange(range1) + " " + x.length() + " " + y.length());
        System.out.println("Canonicalised range to " + RangeCanonicaliser.canonicaliseRange(range2));

        System.out.println("QQQ: " + range1.doesRangeOverlap(range1));

        // When
        Optional<Range> optionalRange = RangeIntersector.intersectRanges(range1, range2, RANGE_FACTORY);

        System.out.println("QQQ2: " + range1 + " " + x + " " + range1.equals(RangeCanonicaliser.canonicaliseRange(range1)));

        // Then
        assertThat(optionalRange).contains(RangeCanonicaliser.canonicaliseRange(range1));
    }
}
