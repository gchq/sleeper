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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RangeTest {

    @Nested
    @DisplayName("Exact object matches")
    class ExactMatch {

        Range range1, range2, range3, range4;

        @BeforeEach
        void setUp() {
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            range1 = rangeFactory.createRange(field, 10, true, 20, true);
            range2 = rangeFactory.createRange(field, 10, true, 20, true);
            range3 = rangeFactory.createRange(field, 10, true, 20, false);
            range4 = rangeFactory.createRange(field, 11, true, 20, false);
        }

        @Test
        public void testEqualsAndHashcode() {
            // When / Then
            assertThat(range1.equals(range2)).isTrue();
            assertThat(range1.equals(range3)).isFalse();
            assertThat(range3.equals(range4)).isFalse();
        }

        @Test
        void testHashCode() {
            // When
            int hashCode1 = range1.hashCode();
            int hashCode2 = range2.hashCode();
            int hashCode3 = range3.hashCode();
            int hashCode4 = range4.hashCode();

            // Then
            assertThat(hashCode2).isEqualTo(hashCode1);
            assertThat(hashCode3).isNotEqualTo(hashCode1);
            assertThat(hashCode4).isNotEqualTo(hashCode3);
        }
    }

    @Nested
    @DisplayName("Canonicalised match")
    class CanonicalisedMatch {
        @Test
        public void testEqualsForCanonicallyIntType() {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range1 = rangeFactory.createRange(field, 10, true, 20, true);
            Range range2 = rangeFactory.createRange(field, 10, false, 20, false);
            Range range3 = rangeFactory.createRange(field, 9, false, 21, false);
            Range range4 = rangeFactory.createRange(field, 9, false, 20, true);
            Range range5 = rangeFactory.createRange(field, 10, true, 21, false);

            // When / Then
            assertThat(range1.equalsCanonicalised(range2)).isFalse();
            assertThat(range1.equalsCanonicalised(range3)).isTrue();
            assertThat(range1.equalsCanonicalised(range4)).isTrue();
            assertThat(range1.equalsCanonicalised(range5)).isTrue();
        }

        @Test
        void shouldEqualsForCanonicallyLongType() {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range1 = rangeFactory.createRange(field, 5L, true, 9L, true);
            Range range2 = rangeFactory.createRange(field, 5L, false, 9L, false);
            Range range3 = rangeFactory.createRange(field, 4L, false, 10L, false);
            Range range4 = rangeFactory.createRange(field, 4L, false, 9L, true);
            Range range5 = rangeFactory.createRange(field, 5L, true, 10L, false);

            // When / Then
            assertThat(range1.equalsCanonicalised(range2)).isFalse();
            assertThat(range1.equalsCanonicalised(range3)).isTrue();
            assertThat(range1.equalsCanonicalised(range4)).isTrue();
            assertThat(range1.equalsCanonicalised(range5)).isTrue();
        }

        @Test
        void shouldEqualsForCanonicallyByteArrayType() {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);

            Range range1 = rangeFactory.createRange(field, new byte[]{10, 10, -128}, true, new byte[]{20, 20}, true);
            Range range2 = rangeFactory.createRange(field, new byte[]{10, 10}, false, new byte[]{20, 20}, false);
            Range range3 = rangeFactory.createRange(field, new byte[]{10, 10}, false, new byte[]{20, 20, -128}, false);
            Range range4 = rangeFactory.createRange(field, new byte[]{10, 10}, false, new byte[]{20, 20}, true);
            Range range5 = rangeFactory.createRange(field, new byte[]{10, 10, -128}, true, new byte[]{20, 20, -128}, false);

            // When / Then
            assertThat(range1.equalsCanonicalised(range2)).isFalse();
            assertThat(range1.equalsCanonicalised(range3)).isTrue();
            assertThat(range1.equalsCanonicalised(range4)).isTrue();
            assertThat(range1.equalsCanonicalised(range5)).isTrue();
        }

        @Test
        void shouldEqualsForCanonicallyStringType() {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range1 = rangeFactory.createRange(field, "A\u0000", true, "AAA", true);
            Range range2 = rangeFactory.createRange(field, "A", false, "AAA", false);
            Range range3 = rangeFactory.createRange(field, "A", false, "AAA\u0000", false);
            Range range4 = rangeFactory.createRange(field, "A", false, "AAA", true);
            Range range5 = rangeFactory.createRange(field, "A\u0000", true, "AAA\u0000", false);

            // When / Then
            assertThat(range1.equalsCanonicalised(range2)).isFalse();
            assertThat(range1.equalsCanonicalised(range3)).isTrue();
            assertThat(range1.equalsCanonicalised(range4)).isTrue();
            assertThat(range1.equalsCanonicalised(range5)).isTrue();
        }
    }

    @Nested
    @DisplayName("Range functionality using IntType")
    class IntTypeRange {
        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeBothInclusive() {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10, true, 20, true);

            // When
            boolean test1 = range.doesRangeContainObject(10);
            boolean test2 = range.doesRangeContainObject(15);
            boolean test3 = range.doesRangeContainObject(20);
            boolean test4 = range.doesRangeContainObject(0);
            boolean test5 = range.doesRangeContainObject(21);

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeBothExclusive() {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10, false, 20, false);

            // When
            boolean test1 = range.doesRangeContainObject(10);
            boolean test2 = range.doesRangeContainObject(15);
            boolean test3 = range.doesRangeContainObject(20);
            boolean test4 = range.doesRangeContainObject(0);
            boolean test5 = range.doesRangeContainObject(21);

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeMinInclusiveMaxExclusive() {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10, true, 20, false);

            // When
            boolean test1 = range.doesRangeContainObject(10);
            boolean test2 = range.doesRangeContainObject(15);
            boolean test3 = range.doesRangeContainObject(20);
            boolean test4 = range.doesRangeContainObject(0);
            boolean test5 = range.doesRangeContainObject(21);

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeMinExclusiveMaxInclusive() {
            // Given
            Field field = new Field("key", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10, false, 20, true);

            // When
            boolean test1 = range.doesRangeContainObject(10);
            boolean test2 = range.doesRangeContainObject(15);
            boolean test3 = range.doesRangeContainObject(20);
            boolean test4 = range.doesRangeContainObject(0);
            boolean test5 = range.doesRangeContainObject(21);

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldGiveCorrectAnswerForDoesRangeOverlapWithIntKey() {
            // Given
            Field field = new Field("key1", new IntType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 1, true, 10, false);

            // When
            //  - Other range is in canonical form, i.e. inclusive of min, exclusive of max
            boolean does1To2OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1, 2));
            boolean does4To5OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 4, 5));
            boolean does1To10OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1, 10));
            boolean doesMinus5To10OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -5, 10));
            boolean doesMinus5To15OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -5, 15));
            boolean doesMinus5To1OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -5, 1));
            boolean doesMinus10ToMinus5OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -10, -5));
            boolean does10To100OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 10, 100));
            //  - Inclusive of min, inclusive of max
            boolean does1To10IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1, true, 10, true));
            boolean does10To11IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 10, true, 11, true));
            boolean doesMinus1To1IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -1, true, 1, true));
            //  - Exclusive of min, exclusive of max
            boolean does1To10ExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1, false, 10, false));
            boolean does9To11ExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 9, false, 11, false));
            //  - Exclusive of min, inclusive of max
            boolean does9To11ExcIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 9, false, 11, true));

            // Then
            assertThat(does1To2OverlapPartition).isTrue();
            assertThat(does4To5OverlapPartition).isTrue();
            assertThat(does1To10OverlapPartition).isTrue();
            assertThat(doesMinus5To10OverlapPartition).isTrue();
            assertThat(doesMinus5To15OverlapPartition).isTrue();
            assertThat(doesMinus5To1OverlapPartition).isFalse();
            assertThat(doesMinus10ToMinus5OverlapPartition).isFalse();
            assertThat(does10To100OverlapPartition).isFalse();
            assertThat(does1To10IncOverlapPartition).isTrue();
            assertThat(does10To11IncOverlapPartition).isFalse();
            assertThat(doesMinus1To1IncOverlapPartition).isTrue();
            assertThat(does1To10ExcOverlapPartition).isTrue();
            assertThat(does9To11ExcOverlapPartition).isFalse();
            assertThat(does9To11ExcIncOverlapPartition).isFalse();
        }
    }

    @Nested
    @DisplayName("Range functionality using LongType")
    class LongTypeRange {
        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeBothInclusive() {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10L, true, 20L, true);

            // When
            boolean test1 = range.doesRangeContainObject(10L);
            boolean test2 = range.doesRangeContainObject(15L);
            boolean test3 = range.doesRangeContainObject(20L);
            boolean test4 = range.doesRangeContainObject(0L);
            boolean test5 = range.doesRangeContainObject(21L);

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeBothExclusive() {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10L, false, 20L, false);

            // When
            boolean test1 = range.doesRangeContainObject(10L);
            boolean test2 = range.doesRangeContainObject(15L);
            boolean test3 = range.doesRangeContainObject(20L);
            boolean test4 = range.doesRangeContainObject(0L);
            boolean test5 = range.doesRangeContainObject(21L);

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeMinInclusiveMaxExclusive() {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10L, true, 20L, false);

            // When
            boolean test1 = range.doesRangeContainObject(10L);
            boolean test2 = range.doesRangeContainObject(15L);
            boolean test3 = range.doesRangeContainObject(20L);
            boolean test4 = range.doesRangeContainObject(0L);
            boolean test5 = range.doesRangeContainObject(21L);

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeMinExclusiveMaxInclusive() {
            // Given
            Field field = new Field("key", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 10L, false, 20L, true);

            // When
            boolean test1 = range.doesRangeContainObject(10L);
            boolean test2 = range.doesRangeContainObject(15L);
            boolean test3 = range.doesRangeContainObject(20L);
            boolean test4 = range.doesRangeContainObject(0L);
            boolean test5 = range.doesRangeContainObject(21L);

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithLongKey() {
            // Given
            Field field = new Field("key1", new LongType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, 1L, true, 10L, false);

            // When
            //  - Other range is in canonical form, i.e. inclusive of min, exclusive of max
            boolean does1To2OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1L, 2L));
            boolean does4To5OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 4L, 5L));
            boolean does1To10OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1L, 10L));
            boolean doesMinus5To10OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -5L, 10L));
            boolean doesMinus5To15OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -5L, 15L));
            boolean doesMinus5To1OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -5L, 1L));
            boolean doesMinus10ToMinus5OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -10L, -5L));
            boolean does10To100OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 10L, 100L));
            //  - Inclusive of min, inclusive of max
            boolean does1To10IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1L, true, 10L, true));
            boolean does10To11IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 10L, true, 11L, true));
            boolean doesMinus1To1IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, -1L, true, 1L, true));
            //  - Exclusive of min, exclusive of max
            boolean does1To10ExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 1L, false, 10L, false));
            boolean does9To11ExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 9L, false, 11L, false));
            //  - Exclusive of min, inclusive of max
            boolean does9To11ExcIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, 9L, false, 11L, true));

            // Then
            assertThat(does1To2OverlapPartition).isTrue();
            assertThat(does4To5OverlapPartition).isTrue();
            assertThat(does1To10OverlapPartition).isTrue();
            assertThat(doesMinus5To10OverlapPartition).isTrue();
            assertThat(doesMinus5To15OverlapPartition).isTrue();
            assertThat(doesMinus5To1OverlapPartition).isFalse();
            assertThat(doesMinus10ToMinus5OverlapPartition).isFalse();
            assertThat(does10To100OverlapPartition).isFalse();
            assertThat(does1To10IncOverlapPartition).isTrue();
            assertThat(does10To11IncOverlapPartition).isFalse();
            assertThat(doesMinus1To1IncOverlapPartition).isTrue();
            assertThat(does1To10ExcOverlapPartition).isTrue();
            assertThat(does9To11ExcOverlapPartition).isFalse();
            assertThat(does9To11ExcIncOverlapPartition).isFalse();
        }
    }

    @Nested
    @DisplayName("Range functionality using StringType")
    class StringTypeRange {

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeBothInclusive() {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, "B", true, "G", true);

            // When
            boolean test1 = range.doesRangeContainObject("B");
            boolean test2 = range.doesRangeContainObject("E");
            boolean test3 = range.doesRangeContainObject("G");
            boolean test4 = range.doesRangeContainObject("A");
            boolean test5 = range.doesRangeContainObject("H");

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeBothExclusive() {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, "B", false, "G", false);

            // When
            boolean test1 = range.doesRangeContainObject("B");
            boolean test2 = range.doesRangeContainObject("E");
            boolean test3 = range.doesRangeContainObject("G");
            boolean test4 = range.doesRangeContainObject("A");
            boolean test5 = range.doesRangeContainObject("H");

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeMinInclusiveMaxExclusive() {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, "B", true, "G", false);

            // When
            boolean test1 = range.doesRangeContainObject("B");
            boolean test2 = range.doesRangeContainObject("E");
            boolean test3 = range.doesRangeContainObject("G");
            boolean test4 = range.doesRangeContainObject("A");
            boolean test5 = range.doesRangeContainObject("H");

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeMinExclusiveMaxInclusive() {
            // Given
            Field field = new Field("key", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, "B", false, "G", true);

            // When
            boolean test1 = range.doesRangeContainObject("B");
            boolean test2 = range.doesRangeContainObject("E");
            boolean test3 = range.doesRangeContainObject("G");
            boolean test4 = range.doesRangeContainObject("A");
            boolean test5 = range.doesRangeContainObject("H");

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithStringKey() {
            // Given
            Field field = new Field("key1", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, "B", true, "G", false);

            // When
            //  - Other range is in canonical form, i.e. inclusive of min, exclusive of max
            boolean doesBToCOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "B", "C"));
            boolean doesCToDOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "C", "D"));
            boolean doesBToGOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "B", "G"));
            boolean doesAToDOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "A", "D"));
            boolean doesDToJOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "D", "J"));
            boolean doesAToBOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "A", "B"));
            boolean doesA1ToA2OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "A1", "A2"));
            boolean doesGToZOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "G", "Z"));
            //  - Inclusive of min, inclusive of max
            boolean doesBToGIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "B", true, "G", true));
            boolean doesGToHIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "G", true, "H", true));
            boolean doesAToBIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "A", true, "B", true));
            //  - Exclusive of min, exclusive of max
            boolean doesBToGExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "B", false, "G", false));
            boolean doesFToHExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "F", false, "H", false));
            //  - Exclusive of min, inclusive of max
            boolean doesFToHExcIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "F", false, "H", true));

            // Then
            assertThat(doesBToCOverlapPartition).isTrue();
            assertThat(doesCToDOverlapPartition).isTrue();
            assertThat(doesBToGOverlapPartition).isTrue();
            assertThat(doesAToDOverlapPartition).isTrue();
            assertThat(doesDToJOverlapPartition).isTrue();
            assertThat(doesAToBOverlapPartition).isFalse();
            assertThat(doesA1ToA2OverlapPartition).isFalse();
            assertThat(doesGToZOverlapPartition).isFalse();
            assertThat(doesBToGIncOverlapPartition).isTrue();
            assertThat(doesGToHIncOverlapPartition).isFalse();
            assertThat(doesAToBIncOverlapPartition).isTrue();
            assertThat(doesBToGExcOverlapPartition).isTrue();
            assertThat(doesFToHExcOverlapPartition).isTrue();
            assertThat(doesFToHExcIncOverlapPartition).isTrue();
        }

        @Test
        public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithStringKeyAndEmptyStringAndNullBoundaries() {
            // Given
            Field field = new Field("key1", new StringType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, "", true, null, false);

            // When
            boolean doesCToDOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "C", "D"));
            boolean doesEmptyToZOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "", "Z"));
            boolean doesXToNullOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "X", null));
            //  - Inclusive of min, inclusive of max
            boolean doesBToBOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "B", true, "B", true));
            boolean doesEmptyStringToNullOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "", true, null, false));
            //  - Exclusive of min, exclusive of max
            boolean doesEmptyStringToEmptyExcStringOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "", false, "B", false));
            //  - Exclusive of min, inclusive of max
            boolean doesEmptyStringToBExcIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, "", false, "B", true));

            // Then
            assertThat(doesCToDOverlapPartition).isTrue();
            assertThat(doesEmptyToZOverlapPartition).isTrue();
            assertThat(doesXToNullOverlapPartition).isTrue();
            assertThat(doesBToBOverlapPartition).isTrue();
            assertThat(doesEmptyStringToNullOverlapPartition).isTrue();
            assertThat(doesEmptyStringToEmptyExcStringOverlapPartition).isTrue();
            assertThat(doesEmptyStringToBExcIncOverlapPartition).isTrue();
        }
    }

    @Nested
    @DisplayName("Range functionality using ByteArrayType")
    class ByteArrayTypeRange {

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeBothInclusive() {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, new byte[]{10, 10}, true, new byte[]{20, 20}, true);

            // When
            boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
            boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
            boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
            boolean test4 = range.doesRangeContainObject(new byte[]{9});
            boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeBothExclusive() {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, new byte[]{10, 10}, false, new byte[]{20, 20}, false);

            // When
            boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
            boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
            boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
            boolean test4 = range.doesRangeContainObject(new byte[]{9});
            boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeMinInclusiveMaxExclusive() {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, new byte[]{10, 10}, true, new byte[]{20, 20}, false);

            // When
            boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
            boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
            boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
            boolean test4 = range.doesRangeContainObject(new byte[]{9});
            boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});

            // Then
            assertThat(test1).isTrue();
            assertThat(test2).isTrue();
            assertThat(test3).isFalse();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeMinExclusiveMaxInclusive() {
            // Given
            Field field = new Field("key", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, new byte[]{10, 10}, false, new byte[]{20, 20}, true);

            // When
            boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
            boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
            boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
            boolean test4 = range.doesRangeContainObject(new byte[]{9});
            boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});

            // Then
            assertThat(test1).isFalse();
            assertThat(test2).isTrue();
            assertThat(test3).isTrue();
            assertThat(test4).isFalse();
            assertThat(test5).isFalse();
        }

        @Test
        public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithByteArrayKey() {
            // Given
            Field field = new Field("key1", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, new byte[]{1, 64}, true, new byte[]{1, 88}, false);

            // When
            boolean does164To165OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 64}, new byte[]{1, 65}));
            boolean does164To168OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 64}, new byte[]{1, 68}));
            boolean does164To188OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 64}, new byte[]{1, 88}));
            boolean does0To170OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{0}, new byte[]{1, 70}));
            boolean does170To299OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 70}, new byte[]{2, 99}));
            boolean does0To164OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{0}, new byte[]{1, 64}));
            boolean does11To11OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 1}, new byte[]{1, 1}));
            boolean does188To9999OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 88}, new byte[]{99, 99}));
            //  - Inclusive of min, inclusive of max
            boolean does164To188IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 64}, true, new byte[]{1, 88}, true));
            boolean does188To2IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 88}, true, new byte[]{2}, true));
            boolean does0To164IncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{0}, true, new byte[]{1, 64}, true));
            //  - Exclusive of min, exclusive of max
            boolean does164To188ExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 64}, false, new byte[]{1, 88}, false));
            boolean does188To2ExcOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 88}, false, new byte[]{2}, false));
            //  - Exclusive of min, inclusive of max
            boolean does187To2ExcIncOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{1, 88}, false, new byte[]{2}, true));

            // Then
            assertThat(does164To165OverlapPartition).isTrue();
            assertThat(does164To168OverlapPartition).isTrue();
            assertThat(does164To188OverlapPartition).isTrue();
            assertThat(does0To170OverlapPartition).isTrue();
            assertThat(does170To299OverlapPartition).isTrue();
            assertThat(does0To164OverlapPartition).isFalse();
            assertThat(does11To11OverlapPartition).isFalse();
            assertThat(does188To9999OverlapPartition).isFalse();
            assertThat(does164To188IncOverlapPartition).isTrue();
            assertThat(does188To2IncOverlapPartition).isFalse();
            assertThat(does0To164IncOverlapPartition).isTrue();
            assertThat(does164To188ExcOverlapPartition).isTrue();
            assertThat(does188To2ExcOverlapPartition).isFalse();
            assertThat(does187To2ExcIncOverlapPartition).isFalse();
        }

        @Test
        public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithByteArrayKeyAndEmptyByteArrayAndNullBoundaries() {
            // Given
            Field field = new Field("key1", new ByteArrayType());
            Schema schema = Schema.builder().rowKeyFields(field).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createRange(field, new byte[]{}, null);

            // When
            boolean does5To5OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{5}, true, new byte[]{5}, true));
            boolean does5To77OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{5}, true, new byte[]{7, 7}, false));
            boolean doesEmptyTo888OverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{}, true, new byte[]{8, 8, 8}, false));
            boolean does888ToNullOverlapPartition = range.doesRangeOverlap(rangeFactory.createRange(field, new byte[]{8, 8, 8}, true, null, false));

            // Then
            assertThat(does5To5OverlapPartition).isTrue();
            assertThat(does5To77OverlapPartition).isTrue();
            assertThat(doesEmptyTo888OverlapPartition).isTrue();
            assertThat(does888ToNullOverlapPartition).isTrue();
        }

    }

    @Test
    public void shouldThrowExceptionIfGivenWrongType() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, 10, true, 20, true);

        // When / Then
        assertThatThrownBy(() -> range.doesRangeContainObject(10L))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    void shouldProvideVisibleIndicationWhenNullCharacterPresentAtEndOfString() {
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Range rangeWithNullCharacters = rangeFactory.createRange(field, "B", false, "G", true);

        // Then 1
        assertThat(RangeCanonicaliser.canonicaliseRange(rangeWithNullCharacters).toString()).isEqualTo(
                "Range{field=Field{name=key, type=StringType{}}, min='B\\u0000', minInclusive=true, max='G\\u0000', maxInclusive=false}");

        // When 2
        Range rangeWithoutNullCharacters = rangeFactory.createRange(field, "T", true, "X", false);

        // Then 2
        assertThat(RangeCanonicaliser.canonicaliseRange(rangeWithoutNullCharacters).toString()).isEqualTo(
                "Range{field=Field{name=key, type=StringType{}}, min='T', minInclusive=true, max='X', maxInclusive=false}");
    }
}
