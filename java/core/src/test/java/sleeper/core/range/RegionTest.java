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

import sleeper.core.key.Key;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class RegionTest {

    private static Schema schemaWithSingleKeyOfType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private static Schema schemaWithTwoKeysOfTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(
                new Field("key1", type1),
                new Field("key2", type2)).build();
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyIntKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new IntType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", 1, true, 2, true));
        Region region2 = new Region(rangeFactory.createRange("key", 1, true, 2, true));
        Region region3 = new Region(rangeFactory.createRange("key", 1, true, 4, true));
        Region region4 = new Region(rangeFactory.createRange("key", 1, true, 4, false));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        boolean equals3 = region3.equals(region4);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();
        int hashCode4 = region4.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(equals3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode3);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyLongKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new LongType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", 1L, true, 2L, true));
        Region region2 = new Region(rangeFactory.createRange("key", 1L, true, 2L, true));
        Region region3 = new Region(rangeFactory.createRange("key", 1L, true, 4L, true));
        Region region4 = new Region(rangeFactory.createRange("key", 1L, true, 4L, false));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        boolean equals3 = region3.equals(region4);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();
        int hashCode4 = region4.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(equals3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode3);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyStringKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", "1", true, "2", true));
        Region region2 = new Region(rangeFactory.createRange("key", "1", true, "2", true));
        Region region3 = new Region(rangeFactory.createRange("key", "1", true, "4", true));
        Region region4 = new Region(rangeFactory.createRange("key", "1", true, "4", false));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        boolean equals3 = region3.equals(region4);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();
        int hashCode4 = region4.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(equals3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode3);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyByteArrayKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new ByteArrayType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{2}, true));
        Region region2 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{2}, true));
        Region region3 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{4}, true));
        Region region4 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{4}, false));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        boolean equals3 = region3.equals(region4);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();
        int hashCode4 = region4.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(equals3).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode3);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyIntKeyNullMax() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new IntType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", 1, true, null, true));
        Region region2 = new Region(rangeFactory.createRange("key", 1, true, null, true));
        Region region3 = new Region(rangeFactory.createRange("key", 1, true, 4, true));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyLongKeyNullMax() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new LongType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", 1L, true, null, true));
        Region region2 = new Region(rangeFactory.createRange("key", 1L, true, null, true));
        Region region3 = new Region(rangeFactory.createRange("key", 1L, true, 4L, true));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyStringKeyNullMax() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", "1", true, null, true));
        Region region2 = new Region(rangeFactory.createRange("key", "1", true, null, true));
        Region region3 = new Region(rangeFactory.createRange("key", "1", true, "4", true));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void equalsAndHashcodeShouldWorkCorrectlyByteArrayKeyNullMax() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new ByteArrayType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, null, true));
        Region region2 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, null, true));
        Region region3 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{4}, true));

        // When
        boolean equals1 = region1.equals(region2);
        boolean equals2 = region1.equals(region3);
        int hashCode1 = region1.hashCode();
        int hashCode2 = region2.hashCode();
        int hashCode3 = region3.hashCode();

        // Then
        assertThat(equals1).isTrue();
        assertThat(equals2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void isKeyInRegionShouldWorkCorrectlyIntKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new IntType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", 1, true, 10, true));
        Region region2 = new Region(rangeFactory.createRange("key", 1, true, 10, false));
        Region region3 = new Region(rangeFactory.createRange("key", 100, true, null, true));
        Key key1 = Key.create(Integer.MIN_VALUE);
        Key key2 = Key.create(1);
        Key key3 = Key.create(5);
        Key key4 = Key.create(10);
        Key key5 = Key.create(100);
        Key key6 = Key.create(Integer.MAX_VALUE);

        // When
        boolean test1 = region1.isKeyInRegion(schema, key1);
        boolean test2 = region1.isKeyInRegion(schema, key2);
        boolean test3 = region1.isKeyInRegion(schema, key3);
        boolean test4 = region1.isKeyInRegion(schema, key4);
        boolean test5 = region1.isKeyInRegion(schema, key5);
        boolean test6 = region2.isKeyInRegion(schema, key4);
        boolean test7 = region3.isKeyInRegion(schema, key5);
        boolean test8 = region3.isKeyInRegion(schema, key6);
        boolean test9 = region3.isKeyInRegion(schema, key4);

        // Then
        assertThat(test1).isFalse();
        assertThat(test2).isTrue();
        assertThat(test3).isTrue();
        assertThat(test4).isTrue();
        assertThat(test5).isFalse();
        assertThat(test6).isFalse();
        assertThat(test7).isTrue();
        assertThat(test8).isTrue();
        assertThat(test9).isFalse();
    }

    @Test
    public void isKeyInRegionShouldWorkCorrectlyLongKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new LongType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", 1L, true, 10L, true));
        Region region2 = new Region(rangeFactory.createRange("key", 1L, true, 10L, false));
        Region region3 = new Region(rangeFactory.createRange("key", 100L, true, null, true));
        Key key1 = Key.create(Long.MIN_VALUE);
        Key key2 = Key.create(1L);
        Key key3 = Key.create(5L);
        Key key4 = Key.create(10L);
        Key key5 = Key.create(100L);
        Key key6 = Key.create(Long.MAX_VALUE);

        // When
        boolean test1 = region1.isKeyInRegion(schema, key1);
        boolean test2 = region1.isKeyInRegion(schema, key2);
        boolean test3 = region1.isKeyInRegion(schema, key3);
        boolean test4 = region1.isKeyInRegion(schema, key4);
        boolean test5 = region1.isKeyInRegion(schema, key5);
        boolean test6 = region2.isKeyInRegion(schema, key4);
        boolean test7 = region3.isKeyInRegion(schema, key5);
        boolean test8 = region3.isKeyInRegion(schema, key6);
        boolean test9 = region3.isKeyInRegion(schema, key4);

        // Then
        assertThat(test1).isFalse();
        assertThat(test2).isTrue();
        assertThat(test3).isTrue();
        assertThat(test4).isTrue();
        assertThat(test5).isFalse();
        assertThat(test6).isFalse();
        assertThat(test7).isTrue();
        assertThat(test8).isTrue();
        assertThat(test9).isFalse();
    }

    @Test
    public void isKeyInRegionShouldWorkCorrectlyStringKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", "1", true, "8", true));
        Region region2 = new Region(rangeFactory.createRange("key", "1", true, "8", false));
        Region region3 = new Region(rangeFactory.createRange("key", "9", true, null, true));
        Key key1 = Key.create("");
        Key key2 = Key.create("1");
        Key key3 = Key.create("5");
        Key key4 = Key.create("8");
        Key key5 = Key.create("9");
        Key key6 = Key.create("99");

        // When
        boolean test1 = region1.isKeyInRegion(schema, key1);
        boolean test2 = region1.isKeyInRegion(schema, key2);
        boolean test3 = region1.isKeyInRegion(schema, key3);
        boolean test4 = region1.isKeyInRegion(schema, key4);
        boolean test5 = region1.isKeyInRegion(schema, key5);
        boolean test6 = region2.isKeyInRegion(schema, key4);
        boolean test7 = region3.isKeyInRegion(schema, key5);
        boolean test8 = region3.isKeyInRegion(schema, key6);
        boolean test9 = region3.isKeyInRegion(schema, key4);

        // Then
        assertThat(test1).isFalse();
        assertThat(test2).isTrue();
        assertThat(test3).isTrue();
        assertThat(test4).isTrue();
        assertThat(test5).isFalse();
        assertThat(test6).isFalse();
        assertThat(test7).isTrue();
        assertThat(test8).isTrue();
        assertThat(test9).isFalse();
    }

    @Test
    public void isKeyInRegionShouldWorkCorrectlyByteArrayKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new ByteArrayType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{10}, true));
        Region region2 = new Region(rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{10}, false));
        Region region3 = new Region(rangeFactory.createRange("key", new byte[]{20}, true, null, true));
        Key key1 = Key.create(new byte[]{});
        Key key2 = Key.create(new byte[]{1});
        Key key3 = Key.create(new byte[]{5});
        Key key4 = Key.create(new byte[]{10});
        Key key5 = Key.create(new byte[]{20});
        Key key6 = Key.create(new byte[]{100});

        // When
        boolean test1 = region1.isKeyInRegion(schema, key1);
        boolean test2 = region1.isKeyInRegion(schema, key2);
        boolean test3 = region1.isKeyInRegion(schema, key3);
        boolean test4 = region1.isKeyInRegion(schema, key4);
        boolean test5 = region1.isKeyInRegion(schema, key5);
        boolean test6 = region2.isKeyInRegion(schema, key4);
        boolean test7 = region3.isKeyInRegion(schema, key5);
        boolean test8 = region3.isKeyInRegion(schema, key6);
        boolean test9 = region3.isKeyInRegion(schema, key4);

        // Then
        assertThat(test1).isFalse();
        assertThat(test2).isTrue();
        assertThat(test3).isTrue();
        assertThat(test4).isTrue();
        assertThat(test5).isFalse();
        assertThat(test6).isFalse();
        assertThat(test7).isTrue();
        assertThat(test8).isTrue();
        assertThat(test9).isFalse();
    }

    @Test
    public void isKeyInRegionShouldWorkCorrectlyMultidimensionalKey() {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, new byte[]{1}, true, new byte[]{10}, true);
        Range range2 = rangeFactory.createRange(field2, 10, true, 12, true);
        Region region = new Region(Arrays.asList(range1, range2));
        Key key1 = Key.create(Arrays.asList(new byte[]{}, 5));
        Key key2 = Key.create(Arrays.asList(new byte[]{}, 10));
        Key key3 = Key.create(Arrays.asList(new byte[]{1}, 5));
        Key key4 = Key.create(Arrays.asList(new byte[]{1}, 10));
        Key key5 = Key.create(Arrays.asList(new byte[]{1}, 20));
        Key key6 = Key.create(Arrays.asList(new byte[]{10}, 11));

        // When
        boolean test1 = region.isKeyInRegion(schema, key1);
        boolean test2 = region.isKeyInRegion(schema, key2);
        boolean test3 = region.isKeyInRegion(schema, key3);
        boolean test4 = region.isKeyInRegion(schema, key4);
        boolean test5 = region.isKeyInRegion(schema, key5);
        boolean test6 = region.isKeyInRegion(schema, key6);

        // Then
        assertThat(test1).isFalse();
        assertThat(test2).isFalse();
        assertThat(test3).isFalse();
        assertThat(test4).isTrue();
        assertThat(test5).isFalse();
        assertThat(test6).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapOneDimIntKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new IntType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range0 = rangeFactory.createRange("key", 1, true, 10, false);
        Region region0 = new Region(range0);

        // When / Then 1
        Range range1 = rangeFactory.createRange("key", 1, true, 2, false);
        Region region1 = new Region(range1);
        assertThat(region0.doesRegionOverlap(region1)).isTrue();

        // When / Then 2
        Range range2 = rangeFactory.createRange("key", -10, true, 1, false);
        Region region2 = new Region(range2);
        assertThat(region0.doesRegionOverlap(region2)).isFalse();

        // When / Then 3
        Range range3 = rangeFactory.createRange("key", 10, true, null, false);
        Region region3 = new Region(range3);
        assertThat(region0.doesRegionOverlap(region3)).isFalse();

        // When / Then 3
        Range range4 = rangeFactory.createRange("key", 9, true, null, false);
        Region region4 = new Region(range4);
        assertThat(region0.doesRegionOverlap(region4)).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapOneDimLongKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new LongType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range0 = rangeFactory.createRange("key", 1L, true, 10L, false);
        Region region0 = new Region(range0);

        // When / Then 1
        Range range1 = rangeFactory.createRange("key", 1L, true, 2L, false);
        Region region1 = new Region(range1);
        assertThat(region0.doesRegionOverlap(region1)).isTrue();

        // When / Then 2
        Range range2 = rangeFactory.createRange("key", -10L, true, 1L, false);
        Region region2 = new Region(range2);
        assertThat(region0.doesRegionOverlap(region2)).isFalse();

        // When / Then 3
        Range range3 = rangeFactory.createRange("key", 10L, true, null, false);
        Region region3 = new Region(range3);
        assertThat(region0.doesRegionOverlap(region3)).isFalse();

        // When / Then 3
        Range range4 = rangeFactory.createRange("key", 9L, true, null, false);
        Region region4 = new Region(range4);
        assertThat(region0.doesRegionOverlap(region4)).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapOneDimStringKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range0 = rangeFactory.createRange("key", "E", true, "P", false);
        Region region0 = new Region(range0);

        // When / Then 1
        Range range1 = rangeFactory.createRange("key", "E", true, "F", false);
        Region region1 = new Region(range1);
        assertThat(region0.doesRegionOverlap(region1)).isTrue();

        // When / Then 2
        Range range2 = rangeFactory.createRange("key", "A", true, "E", false);
        Region region2 = new Region(range2);
        assertThat(region0.doesRegionOverlap(region2)).isFalse();

        // When / Then 3
        Range range3 = rangeFactory.createRange("key", "P", true, null, false);
        Region region3 = new Region(range3);
        assertThat(region0.doesRegionOverlap(region3)).isFalse();

        // When / Then 3
        Range range4 = rangeFactory.createRange("key", "N", true, null, false);
        Region region4 = new Region(range4);
        assertThat(region0.doesRegionOverlap(region4)).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapOneDimByteArrayKey() {
        // Given
        Schema schema = schemaWithSingleKeyOfType(new ByteArrayType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range0 = rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{10}, false);
        Region region0 = new Region(range0);

        // When / Then 1
        Range range1 = rangeFactory.createRange("key", new byte[]{1}, true, new byte[]{9, 8}, false);
        Region region1 = new Region(range1);
        assertThat(region0.doesRegionOverlap(region1)).isTrue();

        // When / Then 2
        Range range2 = rangeFactory.createRange("key", new byte[]{0}, true, new byte[]{1}, false);
        Region region2 = new Region(range2);
        assertThat(region0.doesRegionOverlap(region2)).isFalse();

        // When / Then 3
        Range range3 = rangeFactory.createRange("key", new byte[]{10}, true, null, false);
        Region region3 = new Region(range3);
        assertThat(region0.doesRegionOverlap(region3)).isFalse();

        // When / Then 3
        Range range4 = rangeFactory.createRange("key", new byte[]{9}, true, null, false);
        Region region4 = new Region(range4);
        assertThat(region0.doesRegionOverlap(region4)).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapWithMultidimensionalIntKey() {
        // Given
        Schema schema = schemaWithTwoKeysOfTypes(new IntType(), new IntType());

        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range1 = rangeFactory.createRange("key1", 1, true, 10, false);
        Range range2 = rangeFactory.createRange("key2", 200, true, 300, false);
        Region region0 = new Region(Arrays.asList(range1, range2));

        // When
        // Region 0:
        //      300  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10  (Dimension 1)
        // Region 1:
        //      300  +-----------+
        //           |           |
        //      230  +--+        |
        //           |  | <------+-- range overlaps partition
        //      220  +--+        |
        //           |           |
        //      200  +--+--------+
        //           1  2        10
        Range range11 = rangeFactory.createRange("key1", 1, true, 2, false);
        Range range12 = rangeFactory.createRange("key2", 220, true, 230, false);
        Region region1 = new Region(Arrays.asList(range11, range12));
        boolean doesRegion1OverlapRegion0 = region0.doesRegionOverlap(region1);

        // Region 2:
        //      300  +-----------+
        //           |           |
        //      230  |           |  +---+
        //           |           |  |   | <------ range does not overlap partition
        //      220  |           |  +---+
        //           |           |  11  15
        //      200  +-----------+
        //           1           10
        Range range21 = rangeFactory.createRange("key1", 11, true, 15, false);
        Range range22 = rangeFactory.createRange("key2", 220, true, 230, false);
        Region region2 = new Region(Arrays.asList(range21, range22));
        boolean doesRegion2OverlapRegion0 = region0.doesRegionOverlap(region2);

        // Region 3:
        //                      350 +---+
        //                          |   | <------ range does not overlap partition
        //                      320 +---+
        //                          11  15
        //      300  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range31 = rangeFactory.createRange("key1", 11, true, 15, false);
        Range range32 = rangeFactory.createRange("key2", 320, true, 350, false);
        Region region3 = new Region(Arrays.asList(range31, range32));
        boolean doesRegion3OverlapRegion0 = region0.doesRegionOverlap(region3);

        // Region 4:
        //          350 +---+
        //              |   | <------ range does not overlap partition
        //          320 +---+
        //              2   8
        //      300  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range41 = rangeFactory.createRange("key1", 2, true, 8, false);
        Range range42 = rangeFactory.createRange("key2", 320, true, 350, false);
        Region region4 = new Region(Arrays.asList(range41, range42));
        boolean doesRegion4OverlapRegion0 = region0.doesRegionOverlap(region4);

        // Region 5:
        //      350     +---+
        //              |   | <------ range overlaps partition
        //      300  +--+---+----+
        //           |  |   |    |
        //      280  |  +---+    |
        //           |  2   8    |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range51 = rangeFactory.createRange("key1", 2, true, 8, false);
        Range range52 = rangeFactory.createRange("key2", 280, true, 350, false);
        Region region5 = new Region(Arrays.asList(range51, range52));
        boolean doesRegion5OverlapRegion0 = region0.doesRegionOverlap(region5);

        // Region 6:
        //     350 +---+
        //         |   | <------ range overlaps partition
        //     300 | +-----------+
        //         | | |         |
        //     280 +---+         |
        //        -1 | 2         |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range61 = rangeFactory.createRange("key1", -1, true, 2, false);
        Range range62 = rangeFactory.createRange("key2", 280, true, 350, false);
        Region region6 = new Region(Arrays.asList(range61, range62));
        boolean doesRegion6OverlapRegion0 = region0.doesRegionOverlap(region6);

        // Region 7:
        // 350 +---+
        //     |   | <------ range does not overlap partition
        // 300 |   | +-----------+
        //     |   | |           |
        // 280 +---+ |           |
        //    -2   0 |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range71 = rangeFactory.createRange("key1", -2, true, 0, false);
        Range range72 = rangeFactory.createRange("key2", 280, true, 350, false);
        Region region7 = new Region(Arrays.asList(range71, range72));
        boolean doesRegion7OverlapRegion0 = region0.doesRegionOverlap(region7);

        //  Region 8:
        //320 +-------------------------+
        //    |                         |
        //    | 300  +-----------+      |
        //    |      |           |      |
        //    |      |           |      | <------ range overlaps partition
        //    |      |           |      |
        //    |      |           |      |
        //    | 200  +-----------+      |
        //    |      1           10     |
        //    |                         |
        //150 +-------------------------+
        //    -1                        12
        Range range81 = rangeFactory.createRange("key1", -1, true, 12, false);
        Range range82 = rangeFactory.createRange("key2", 150, true, 320, false);
        Region region8 = new Region(Arrays.asList(range81, range82));
        boolean doesRegion8OverlapRegion0 = region0.doesRegionOverlap(region8);

        // Region 9:
        //      300  +-----------+
        //      290  |           +----+
        //           |           |    | <------ range does not overlap partition as partitions do not contain their max
        //           |           |    |
        //      230  |           +----+
        //           |           |    15
        //      200  +-----------+
        //           1           10
        Range range91 = rangeFactory.createRange("key1", 10, true, 15, false);
        Range range92 = rangeFactory.createRange("key2", 230, true, 290, false);
        Region region9 = new Region(Arrays.asList(range91, range92));
        boolean doesRegion9OverlapRegion0 = region0.doesRegionOverlap(region9);

        // Region 10 - Region is inclusive of min, inclusive of max:
        // 350 +-----+
        //     |     | <------ range does overlap partition
        // 300 |     +-----------+
        //     |     |           |
        // 280 +-----+           |
        //    -2     |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range101 = rangeFactory.createRange("key1", -2, true, 1, true);
        Range range102 = rangeFactory.createRange("key2", 280, true, 350, true);
        Region region10 = new Region(Arrays.asList(range101, range102));
        boolean doesRegion10OverlapRegion0 = region0.doesRegionOverlap(region10);

        // Region 11 - Region is exclusive of min, exclusive of max:
        // 350 +-----+
        //     |     | <------ range does not overlap partition
        // 300 |     +-----------+
        //     |     |           |
        // 280 +-----+           |
        //    -2     |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range111 = rangeFactory.createRange("key1", -2, false, 1, false);
        Range range112 = rangeFactory.createRange("key2", 280, false, 350, false);
        Region region11 = new Region(Arrays.asList(range111, range112));
        boolean doesRegion11OverlapRegion0 = region0.doesRegionOverlap(region11);

        // Region 12 - Region is exclusive of min, inclusive of max:
        // 350 +-----+
        //     |     | <------ range does overlap partition
        // 300 |     +-----------+
        //     |     |           |
        // 280 +-----+           |
        //    -2     |           |
        //           |           |
        //           |           |
        //      200  +-----------+
        //           1           10
        Range range121 = rangeFactory.createRange("key1", -2, false, 1, true);
        Range range122 = rangeFactory.createRange("key2", 280, false, 350, true);
        Region region12 = new Region(Arrays.asList(range121, range122));
        boolean doesRegion12OverlapRegion0 = region0.doesRegionOverlap(region12);

        // Then
        assertThat(doesRegion1OverlapRegion0).isTrue();
        assertThat(doesRegion2OverlapRegion0).isFalse();
        assertThat(doesRegion3OverlapRegion0).isFalse();
        assertThat(doesRegion4OverlapRegion0).isFalse();
        assertThat(doesRegion5OverlapRegion0).isTrue();
        assertThat(doesRegion6OverlapRegion0).isTrue();
        assertThat(doesRegion7OverlapRegion0).isFalse();
        assertThat(doesRegion8OverlapRegion0).isTrue();
        assertThat(doesRegion9OverlapRegion0).isFalse();
        assertThat(doesRegion10OverlapRegion0).isTrue();
        assertThat(doesRegion11OverlapRegion0).isFalse();
        assertThat(doesRegion12OverlapRegion0).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapWithMultidimensionalStringKey() {
        // Given
        Schema schema = schemaWithTwoKeysOfTypes(new StringType(), new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range1 = rangeFactory.createRange("key1", "A", true, "E", false);
        Range range2 = rangeFactory.createRange("key2", "P", true, "V", false);
        Region region0 = new Region(Arrays.asList(range1, range2));

        // When
        // Region 0:
        //        V  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E  (Dimension 1)
        // Region 1:
        //        V  +-----------+
        //           |           |
        //        T  +--+        |
        //           |  | <------+-- range overlaps partition
        //        R  +--+        |
        //           |           |
        //        P  +--+--------+
        //           A  C        E
        Range range11 = rangeFactory.createRange("key1", "A", true, "C", false);
        Range range12 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region1 = new Region(Arrays.asList(range11, range12));
        boolean doesRegion1OverlapRegion0 = region0.doesRegionOverlap(region1);

        // Region 2:
        //        V  +-----------+
        //           |           |
        //        T  |           |  +---+
        //           |           |  |   | <------ range does not overlap partition
        //        R  |           |  +---+
        //           |           |  F   H
        //        P  +-----------+
        //           A           E
        Range range21 = rangeFactory.createRange("key1", "F", true, "H", false);
        Range range22 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region2 = new Region(Arrays.asList(range21, range22));
        boolean doesRegion2OverlapRegion0 = region0.doesRegionOverlap(region2);

        // Region 3:
        //                        Z +---+
        //                          |   | <------ range does not overlap partition
        //                        X +---+
        //                          F   H
        //        V  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range31 = rangeFactory.createRange("key1", "F", true, "H", false);
        Range range32 = rangeFactory.createRange("key2", "X", true, "Z", false);
        Region region3 = new Region(Arrays.asList(range31, range32));
        boolean doesRegion3OverlapRegion0 = region0.doesRegionOverlap(region3);

        // Region 4:
        //            Z +---+
        //              |   | <------ range does not overlap partition
        //            X +---+
        //              B   D
        //        V  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range41 = rangeFactory.createRange("key1", "B", true, "D", false);
        Range range42 = rangeFactory.createRange("key2", "X", true, "Z", false);
        Region region4 = new Region(Arrays.asList(range41, range42));
        boolean doesRegion4OverlapRegion0 = region0.doesRegionOverlap(region4);

        // Region 5:
        //        Z     +---+
        //              |   | <------ range overlaps partition
        //        V  +--+---+----+
        //           |  |   |    |
        //        R  |  +---+    |
        //           |  B   D    |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range51 = rangeFactory.createRange("key1", "B", true, "D", false);
        Range range52 = rangeFactory.createRange("key2", "R", true, "Z", false);
        Region region5 = new Region(Arrays.asList(range51, range52));
        boolean doesRegion5OverlapRegion0 = region0.doesRegionOverlap(region5);

        // Region 6:
        //       Z +---+
        //         |   | <------ range overlaps partition
        //       V | +-----------+
        //         | | |         |
        //       R +---+         |
        //         0 | B         |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range61 = rangeFactory.createRange("key1", "0", true, "B", false);
        Range range62 = rangeFactory.createRange("key2", "R", true, "Z", false);
        Region region6 = new Region(Arrays.asList(range61, range62));
        boolean doesRegion6OverlapRegion0 = region0.doesRegionOverlap(region6);

        // Region 7:
        //   Z +---+
        //     |   | <------ range does not overlap partition
        //   V |   | +-----------+
        //     |   | |           |
        //   R +---+ |           |
        //    -2   0 |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range71 = rangeFactory.createRange("key1", "-2", true, "0", false);
        Range range72 = rangeFactory.createRange("key2", "R", true, "Z", false);
        Region region7 = new Region(Arrays.asList(range71, range72));
        boolean doesRegion7OverlapRegion0 = region0.doesRegionOverlap(region7);

        //  Region 8:
        //  Z +-------------------------+
        //    |                         |
        //    |   V  +-----------+      |
        //    |      |           |      |
        //    |      |           |      | <------ range overlaps partition
        //    |      |           |      |
        //    |      |           |      |
        //    |   P  +-----------+      |
        //    |      A           E      |
        //    |                         |
        //  0 +-------------------------+
        //    0                         H
        Range range81 = rangeFactory.createRange("key1", "0", true, "H", false);
        Range range82 = rangeFactory.createRange("key2", "0", true, "Z", false);
        Region region8 = new Region(Arrays.asList(range81, range82));
        boolean doesRegion8OverlapRegion0 = region0.doesRegionOverlap(region8);

        // Region 9:
        //        V  +-----------+
        //        T  |           +----+
        //           |           |    | <------ range does not overlap partition as partitions do not contain their max
        //           |           |    |
        //        R  |           +----+
        //           |           |    H
        //        P  +-----------+
        //           A           E
        Range range91 = rangeFactory.createRange("key1", "E", true, "H", false);
        Range range92 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region9 = new Region(Arrays.asList(range91, range92));
        boolean doesRegion9OverlapRegion0 = region0.doesRegionOverlap(region9);

        // Region 10 - Region is inclusive of min, inclusive of max:
        //   Z +-----+
        //     |     | <------ range does overlap partition
        //   V |     +-----------+
        //     |     |           |
        //   R +-----+           |
        //    -2     |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range101 = rangeFactory.createRange("key1", "-2", true, "A", true);
        Range range102 = rangeFactory.createRange("key2", "R", true, "Z", true);
        Region region10 = new Region(Arrays.asList(range101, range102));
        boolean doesRegion10OverlapRegion0 = region0.doesRegionOverlap(region10);

        // Region 11 - Region is exclusive of min, exclusive of max:
        //   Z +-----+
        //     |     | <------ range does not overlap partition
        //   V |     +-----------+
        //     |     |           |
        //   R +-----+           |
        //    -2     |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range111 = rangeFactory.createRange("key1", "-2", false, "A", false);
        Range range112 = rangeFactory.createRange("key2", "R", false, "Z", false);
        Region region11 = new Region(Arrays.asList(range111, range112));
        boolean doesRegion11OverlapRegion0 = region0.doesRegionOverlap(region11);

        // Region 12 - Region is exclusive of min, inclusive of max:
        //   Z +-----+
        //     |     | <------ range does overlap partition
        //   V |     +-----------+
        //     |     |           |
        //   R +-----+           |
        //    -2     |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           E
        Range range121 = rangeFactory.createRange("key1", "-2", false, "A", true);
        Range range122 = rangeFactory.createRange("key2", "R", false, "Z", true);
        Region region12 = new Region(Arrays.asList(range121, range122));
        boolean doesRegion12OverlapRegion0 = region0.doesRegionOverlap(region12);

        // Then
        assertThat(doesRegion1OverlapRegion0).isTrue();
        assertThat(doesRegion2OverlapRegion0).isFalse();
        assertThat(doesRegion3OverlapRegion0).isFalse();
        assertThat(doesRegion4OverlapRegion0).isFalse();
        assertThat(doesRegion5OverlapRegion0).isTrue();
        assertThat(doesRegion6OverlapRegion0).isTrue();
        assertThat(doesRegion7OverlapRegion0).isFalse();
        assertThat(doesRegion8OverlapRegion0).isTrue();
        assertThat(doesRegion9OverlapRegion0).isFalse();
        assertThat(doesRegion10OverlapRegion0).isTrue();
        assertThat(doesRegion11OverlapRegion0).isFalse();
        assertThat(doesRegion12OverlapRegion0).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoRegionsOverlapWithMultidimensionalStringKeyAndNullBoundaries() {
        // Given
        Schema schema = schemaWithTwoKeysOfTypes(new StringType(), new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range1 = rangeFactory.createRange("key1", "A", true, null, false);
        Range range2 = rangeFactory.createRange("key2", "P", true, null, false);
        Region region0 = new Region(Arrays.asList(range1, range2));

        // When
        // Region 0:
        //     null  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           null (i.e. infinity)  (Dimension 1)
        // Region 1:
        //     null  +-----------+
        //           |           |
        //        T  +--+        |
        //           |  | <------+-- range overlaps partition
        //        R  +--+        |
        //           |           |
        //        P  +--+--------+
        //           A  C        null
        Range range11 = rangeFactory.createRange("key1", "A", true, "C", false);
        Range range12 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region1 = new Region(Arrays.asList(range11, range12));
        boolean doesRegion1OverlapRegion0 = region0.doesRegionOverlap(region1);

        // Region 2:
        //     null  +--+---+----+
        //        Z  |  +---+    |
        //           |  |   | <--|----- range overlaps partition
        //           |  |   |    |
        //        R  |  +---+    |
        //           |  B   D    |
        //           |           |
        //        P  +-----------+
        //           A           null
        Range range21 = rangeFactory.createRange("key1", "B", true, "D", false);
        Range range22 = rangeFactory.createRange("key2", "R", true, "Z", false);
        Region region2 = new Region(Arrays.asList(range21, range22));
        boolean doesRegion2OverlapRegion0 = region0.doesRegionOverlap(region2);

        // Region 3:
        //   null  +-+-+---------+
        //         | | |         |
        //       R +---+ <-------|----- range overlaps partition
        //         0 | B         |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           A           null
        Range range31 = rangeFactory.createRange("key1", "0", true, "B", false);
        Range range32 = rangeFactory.createRange("key2", "R", true, null, false);
        Region region3 = new Region(Arrays.asList(range31, range32));
        boolean doesRegion3OverlapRegion0 = region0.doesRegionOverlap(region3);

        // Region 4:
        //        <------ range does not overlap partition
        //null +---+ +-----------+
        //     |   | |           |
        //     |   | |           |
        //     |   | |           |
        //   R +---+ |           |
        //    -2   0 |           |
        //        P  +-----------+
        //           A           null
        Range range41 = rangeFactory.createRange("key1", "-2", true, "0", false);
        Range range42 = rangeFactory.createRange("key2", "R", true, null, false);
        Region region4 = new Region(Arrays.asList(range41, range42));
        boolean doesRegion4OverlapRegion0 = region0.doesRegionOverlap(region4);

        //  Region 5:
        //    +-null-+-----------+
        //    |      |           |
        //    |      |           | <------ range overlaps partition
        //    |      |           |
        //    |      |           |
        //    +---P--+-----------+
        //    0      A           null
        Range range51 = rangeFactory.createRange("key1", "0", true, null, false);
        Range range52 = rangeFactory.createRange("key2", "P", true, null, false);
        Region region5 = new Region(Arrays.asList(range51, range52));
        boolean doesRegion5OverlapRegion0 = region0.doesRegionOverlap(region5);

        // Then
        assertThat(doesRegion1OverlapRegion0).isTrue();
        assertThat(doesRegion2OverlapRegion0).isTrue();
        assertThat(doesRegion3OverlapRegion0).isTrue();
        assertThat(doesRegion4OverlapRegion0).isFalse();
        assertThat(doesRegion5OverlapRegion0).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithMultidimensionalByteArrayKey() {
        // Given
        Schema schema = schemaWithTwoKeysOfTypes(new ByteArrayType(), new ByteArrayType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range1 = rangeFactory.createRange("key1", new byte[]{5}, true, new byte[]{10}, false);
        Range range2 = rangeFactory.createRange("key2", new byte[]{20}, true, new byte[]{30}, false);
        Region region0 = new Region(Arrays.asList(range1, range2));

        // When
        // Region 0:
        //     [30]  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //     [20]  +-----------+
        //           [5]         [10]  (Dimension 1)
        // Region 1:
        //     [30]  +-----------+
        //           |           |
        //     [26]  +--+        |
        //           |  | <------+-- range overlaps partition
        //     [22]  +--+        |
        //           |           |
        //     [20]  +--+--------+
        //         [5]  [7]      [10]
        Range range11 = rangeFactory.createRange("key1", new byte[]{5}, true, new byte[]{7}, false);
        Range range12 = rangeFactory.createRange("key2", new byte[]{22}, true, new byte[]{26}, false);
        Region region1 = new Region(Arrays.asList(range11, range12));
        boolean doesRegion1OverlapRegion0 = region0.doesRegionOverlap(region1);

        // Region 2:
        //     [30]  +-----------+
        //           |           |
        //     [26]  |           |  +---+
        //           |           |  |   | <------ range does not overlap partition
        //     [22]  |           |  +---+
        //           |           | [12] [15]
        //     [20]  +-----------+
        //           [5]         [10]
        Range range21 = rangeFactory.createRange("key1", new byte[]{12}, true, new byte[]{15}, false);
        Range range22 = rangeFactory.createRange("key2", new byte[]{22}, true, new byte[]{26}, false);
        Region region2 = new Region(Arrays.asList(range21, range22));
        boolean doesRegion2OverlapRegion0 = region0.doesRegionOverlap(region2);

        // Region 3:
        //                     [40] +---+
        //                          |   | <------ range does not overlap partition
        //                     [35] +---+
        //                         [12] [15]
        //     [30]  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //     [20]  +-----------+
        //           [5]         [10]
        Range range31 = rangeFactory.createRange("key1", new byte[]{12}, true, new byte[]{15}, false);
        Range range32 = rangeFactory.createRange("key2", new byte[]{35}, true, new byte[]{40}, false);
        Region region3 = new Region(Arrays.asList(range31, range32));
        boolean doesRegion3OverlapRegion0 = region0.doesRegionOverlap(region3);

        // Region 4:
        //         [40] +---+
        //              |   | <------ range does not overlap partition
        //         [35] +---+
        //             [6]  [8]
        //     [30]  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //     [20]  +-----------+
        //           [5]         [10]
        Range range41 = rangeFactory.createRange("key1", new byte[]{6}, true, new byte[]{8}, false);
        Range range42 = rangeFactory.createRange("key2", new byte[]{35}, true, new byte[]{40}, false);
        Region region4 = new Region(Arrays.asList(range41, range42));
        boolean doesRegion4OverlapRegion0 = region0.doesRegionOverlap(region4);

        // Region 5:
        //     [35]     +---+
        //              |   | <------ range overlaps partition
        //     [30]  +--+---+----+
        //           |  |   |    |
        //     [25]  |  +---+    |
        //           | [6] [8]   |
        //           |           |
        //           |           |
        //     [20]  +-----------+
        //           [5]         [10]
        Range range51 = rangeFactory.createRange("key1", new byte[]{6}, true, new byte[]{8}, false);
        Range range52 = rangeFactory.createRange("key2", new byte[]{25}, true, new byte[]{35}, false);
        Region region5 = new Region(Arrays.asList(range51, range52));
        boolean doesRegion5OverlapRegion0 = region0.doesRegionOverlap(region5);

        // Region 6:
        //    [35] +---+
        //         |   | <------ range overlaps partition
        //    [30] | +-----------+
        //         | | |         |
        //    [25] +---+         |
        //        [4]| [6]       |
        //           |           |
        //           |           |
        //     [20]  +-----------+
        //           [5]         [10]
        Range range61 = rangeFactory.createRange("key1", new byte[]{4}, true, new byte[]{6}, false);
        Range range62 = rangeFactory.createRange("key2", new byte[]{25}, true, new byte[]{35}, false);
        Region region6 = new Region(Arrays.asList(range61, range62));
        boolean doesRegion6OverlapRegion0 = region0.doesRegionOverlap(region6);

        // Region 7:
        // [35]+---+
        //     |   | <------ range does not overlap partition
        // [30]|   | +-----------+
        //     |   | |           |
        // [25]+---+ |           |
        //    [1] [4]|           |
        //           |           |
        //           |           |
        //     [20]  +-----------+
        //           [5]         [10]
        Range range71 = rangeFactory.createRange("key1", new byte[]{1}, true, new byte[]{4}, false);
        Range range72 = rangeFactory.createRange("key2", new byte[]{25}, true, new byte[]{35}, false);
        Region region7 = new Region(Arrays.asList(range71, range72));
        boolean doesRegion7OverlapRegion0 = region0.doesRegionOverlap(region7);

        //  Region 8:
        //[40]+-------------------------+
        //    |                         |
        //    | [30] +-----------+      |
        //    |      |           |      |
        //    |      |           |      | <------ range overlaps partition
        //    |      |           |      |
        //    |      |           |      |
        //    | [20] +-----------+      |
        //    |      [5]         [10]   |
        //    |                         |
        //[10]+-------------------------+
        //    [0]                       [20]
        Range range81 = rangeFactory.createRange("key1", new byte[]{0}, true, new byte[]{20}, false);
        Range range82 = rangeFactory.createRange("key2", new byte[]{10}, true, new byte[]{40}, false);
        Region region8 = new Region(Arrays.asList(range81, range82));
        boolean doesRegion8OverlapRegion0 = region0.doesRegionOverlap(region8);

        // Region 9:
        //     [40]  +-----------+
        //     [38]  |           +----+
        //           |           |    | <------ range does not overlap partition as partitions do not contain their max
        //           |           |    |
        //     [26]  |           +----+
        //           |           |    [25]
        //     [20]  +-----------+
        //           [0]         [20]
        Range range91 = rangeFactory.createRange("key1", new byte[]{20}, true, new byte[]{25}, false);
        Range range92 = rangeFactory.createRange("key2", new byte[]{26}, true, new byte[]{38}, false);
        Region region9 = new Region(Arrays.asList(range91, range92));
        boolean doesRegion9OverlapRegion0 = region0.doesRegionOverlap(region9);

        // Then
        assertThat(doesRegion1OverlapRegion0).isTrue();
        assertThat(doesRegion2OverlapRegion0).isFalse();
        assertThat(doesRegion3OverlapRegion0).isFalse();
        assertThat(doesRegion4OverlapRegion0).isFalse();
        assertThat(doesRegion5OverlapRegion0).isTrue();
        assertThat(doesRegion6OverlapRegion0).isTrue();
        assertThat(doesRegion7OverlapRegion0).isFalse();
        assertThat(doesRegion8OverlapRegion0).isTrue();
        assertThat(doesRegion9OverlapRegion0).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithMultidimensionalIntAndStringKey() {
        // Given
        Schema schema = schemaWithTwoKeysOfTypes(new IntType(), new StringType());
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Range range1 = rangeFactory.createRange("key1", 4, true, 10, false);
        Range range2 = rangeFactory.createRange("key2", "P", true, "V", false);
        Region region0 = new Region(Arrays.asList(range1, range2));

        // When
        // Region 0:
        //        V  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           4           10  (Dimension 1)
        // Region 1:
        //        V  +-----------+
        //           |           |
        //        T  +--+        |
        //           |  | <------+-- range overlaps partition
        //        R  +--+        |
        //           |           |
        //        P  +--+--------+
        //           4  6        10
        Range range11 = rangeFactory.createRange("key1", 4, true, 6, false);
        Range range12 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region1 = new Region(Arrays.asList(range11, range12));
        boolean doesRegion1OverlapRegion0 = region0.doesRegionOverlap(region1);

        // Region 2:
        //        V  +-----------+
        //           |           |
        //        T  |           |  +---+
        //           |           |  |   | <------ range does not overlap partition
        //        R  |           |  +---+
        //           |           |  12  14
        //        P  +-----------+
        //           4           10
        Range range21 = rangeFactory.createRange("key1", 12, true, 14, false);
        Range range22 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region2 = new Region(Arrays.asList(range21, range22));
        boolean doesRegion2OverlapRegion0 = region0.doesRegionOverlap(region2);

        // Region 3:
        //                        Z +---+
        //                          |   | <------ range does not overlap partition
        //                        X +---+
        //                          12  14
        //        V  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           4           10
        Range range31 = rangeFactory.createRange("key1", 12, true, 14, false);
        Range range32 = rangeFactory.createRange("key2", "X", true, "Z", false);
        Region region3 = new Region(Arrays.asList(range31, range32));
        boolean doesRegion3OverlapRegion0 = region0.doesRegionOverlap(region3);

        // Region 4:
        //            Z +---+
        //              |   | <------ range does not overlap partition
        //            X +---+
        //              6   8
        //        V  +-----------+
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           4           10
        Range range41 = rangeFactory.createRange("key1", 6, true, 8, false);
        Range range42 = rangeFactory.createRange("key2", "X", true, "Z", false);
        Region region4 = new Region(Arrays.asList(range41, range42));
        boolean doesRegion4OverlapRegion0 = region0.doesRegionOverlap(region4);

        // Region 5:
        //        Z     +---+
        //              |   | <------ range overlaps partition
        //        V  +--+---+----+
        //           |  |   |    |
        //        T  |  +---+    |
        //           |  6   8    |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           4           10
        Range range51 = rangeFactory.createRange("key1", 6, true, 8, false);
        Range range52 = rangeFactory.createRange("key2", "T", true, "Z", false);
        Region region5 = new Region(Arrays.asList(range51, range52));
        boolean doesRegion5OverlapRegion0 = region0.doesRegionOverlap(region5);

        // Region 6:
        //       Z +---+
        //         |   | <------ range overlaps partition
        //       V | +-----------+
        //         | | |         |
        //       R +---+         |
        //         0 | 5         |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           4           10
        Range range61 = rangeFactory.createRange("key1", 0, true, 5, false);
        Range range62 = rangeFactory.createRange("key2", "R", true, "Z", false);
        Region region6 = new Region(Arrays.asList(range61, range62));
        boolean doesRegion6OverlapRegion0 = region0.doesRegionOverlap(region6);

        // Region 7:
        //   Z +---+
        //     |   | <------ range does not overlap partition
        //   V |   | +-----------+
        //     |   | |           |
        //   R +---+ |           |
        //    -2   0 |           |
        //           |           |
        //           |           |
        //        P  +-----------+
        //           4           10
        Range range71 = rangeFactory.createRange("key1", -2, true, 0, false);
        Range range72 = rangeFactory.createRange("key2", "R", true, "Z", false);
        Region region7 = new Region(Arrays.asList(range71, range72));
        boolean doesRegion7OverlapRegion0 = region0.doesRegionOverlap(region7);

        //  Region 8:
        //  Z +-------------------------+
        //    |                         |
        //    |   V  +-----------+      |
        //    |      |           |      |
        //    |      |           |      | <------ range overlaps partition
        //    |      |           |      |
        //    |      |           |      |
        //    |   P  +-----------+      |
        //    |      4           10     |
        //    |                         |
        //  A +-------------------------+
        //    0                         20
        Range range81 = rangeFactory.createRange("key1", 0, true, 20, false);
        Range range82 = rangeFactory.createRange("key2", "A", true, "Z", false);
        Region region8 = new Region(Arrays.asList(range81, range82));
        boolean doesRegion8OverlapRegion0 = region0.doesRegionOverlap(region8);

        // Region 9:
        //        V  +-----------+
        //        T  |           +----+
        //           |           |    | <------ range does not overlap partition as partitions do not contain their max
        //           |           |    |
        //        R  |           +----+
        //           |           |    15
        //        P  +-----------+
        //           4           10
        Range range91 = rangeFactory.createRange("key1", 10, true, 15, false);
        Range range92 = rangeFactory.createRange("key2", "R", true, "T", false);
        Region region9 = new Region(Arrays.asList(range91, range92));
        boolean doesRegion9OverlapRegion0 = region0.doesRegionOverlap(region9);

        // Then
        assertThat(doesRegion1OverlapRegion0).isTrue();
        assertThat(doesRegion2OverlapRegion0).isFalse();
        assertThat(doesRegion3OverlapRegion0).isFalse();
        assertThat(doesRegion4OverlapRegion0).isFalse();
        assertThat(doesRegion5OverlapRegion0).isTrue();
        assertThat(doesRegion6OverlapRegion0).isTrue();
        assertThat(doesRegion7OverlapRegion0).isFalse();
        assertThat(doesRegion8OverlapRegion0).isTrue();
        assertThat(doesRegion9OverlapRegion0).isFalse();
    }
}
