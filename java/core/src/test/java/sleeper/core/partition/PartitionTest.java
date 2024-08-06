/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PartitionTest {

    private Partition createMidPartition(Schema schema, Object splitPoint1, Object splitPoint2) {
        return PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                Arrays.asList("C1", "C2", "C3"),
                Arrays.asList(splitPoint1, splitPoint2))
                .parentJoining("M", "C1", "C2")
                .parentJoining("P", "M", "C3")
                .buildTree().getPartition("M");
    }

    @Test
    public void testEqualsAndHashcodeWithIntKeyWithNulls() {
        // Given

        Field field1 = new Field("key", new IntType());
        Schema schema1 = Schema.builder().rowKeyFields(field1).build();
        Partition partition1 = createMidPartition(schema1, 10, 100);

        Field field2 = new Field("key", new IntType());
        Schema schema2 = Schema.builder().rowKeyFields(field2).build();
        Partition partition2 = createMidPartition(schema2, 10, 100);

        Field field3 = new Field("key", new IntType());
        Schema schema3 = Schema.builder().rowKeyFields(field3).build();
        Partition partition3 = createMidPartition(schema3, 10, 200);

        // When
        boolean test1 = partition1.equals(partition2);
        boolean test2 = partition1.equals(partition3);
        int hashCode1 = partition1.hashCode();
        int hashCode2 = partition2.hashCode();
        int hashCode3 = partition3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithLongKey() {
        // Given

        Field field1 = new Field("key", new LongType());
        Schema schema1 = Schema.builder().rowKeyFields(field1).build();
        Partition partition1 = createMidPartition(schema1, 0L, 100L);

        Field field2 = new Field("key", new LongType());
        Schema schema2 = Schema.builder().rowKeyFields(field2).build();
        Partition partition2 = createMidPartition(schema2, 0L, 100L);

        Field field3 = new Field("key", new LongType());
        Schema schema3 = Schema.builder().rowKeyFields(field3).build();
        Partition partition3 = createMidPartition(schema3, 0L, 200L);

        // When
        boolean test1 = partition1.equals(partition2);
        boolean test2 = partition1.equals(partition3);
        int hashCode1 = partition1.hashCode();
        int hashCode2 = partition2.hashCode();
        int hashCode3 = partition3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithLongKeyDifferentTypes() {
        // Given
        Field field1 = new Field("key", new LongType());
        Schema schema1 = Schema.builder().rowKeyFields(field1).build();
        Partition partition1 = createMidPartition(schema1, 0L, 100L);

        Field field2 = new Field("key", new LongType());
        Schema schema2 = Schema.builder().rowKeyFields(field2).build();
        Partition partition2 = createMidPartition(schema2, 0L, 100L);

        Field field3 = new Field("key", new ByteArrayType());
        Schema schema3 = Schema.builder().rowKeyFields(field3).build();
        Partition partition3 = createMidPartition(schema3, new byte[]{0, 1}, new byte[]{101, 1});

        // When
        boolean test1 = partition1.equals(partition2);
        boolean test2 = partition1.equals(partition3);
        int hashCode1 = partition1.hashCode();
        int hashCode2 = partition2.hashCode();
        int hashCode3 = partition3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithByteArrayKey() {
        // Given
        Field field1 = new Field("key", new ByteArrayType());
        Schema schema1 = Schema.builder().rowKeyFields(field1).build();
        Partition partition1 = createMidPartition(schema1, new byte[]{0, 1}, new byte[]{100, 1});

        Field field2 = new Field("key", new ByteArrayType());
        Schema schema2 = Schema.builder().rowKeyFields(field2).build();
        Partition partition2 = createMidPartition(schema2, new byte[]{0, 1}, new byte[]{100, 1});

        Field field3 = new Field("key", new ByteArrayType());
        Schema schema3 = Schema.builder().rowKeyFields(field3).build();
        Partition partition3 = createMidPartition(schema3, new byte[]{0, 1}, new byte[]{101, 1});

        // When
        boolean test1 = partition1.equals(partition2);
        boolean test2 = partition1.equals(partition3);
        int hashCode1 = partition1.hashCode();
        int hashCode2 = partition2.hashCode();
        int hashCode3 = partition3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithMultidimensionalRegion() {
        // Given
        Field field11 = new Field("key1", new ByteArrayType());
        Field field12 = new Field("key2", new ByteArrayType());
        Schema schema1 = Schema.builder().rowKeyFields(field11, field12).build();
        RangeFactory rangeFactory1 = new RangeFactory(schema1);
        List<Range> ranges1 = new ArrayList<>();
        ranges1.add(rangeFactory1.createRange(field11, new byte[]{0, 1}, true, new byte[]{99}, false));
        ranges1.add(rangeFactory1.createRange(field12, new byte[]{100, 99}, true, new byte[]{101, 99, 99}, false));
        Region region1 = new Region(ranges1);
        Partition partition1 = Partition.builder()
                .id("id1")
                .region(region1)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        Field field21 = new Field("key1", new ByteArrayType());
        Field field22 = new Field("key2", new ByteArrayType());
        Schema schema2 = Schema.builder().rowKeyFields(field21, field22).build();
        RangeFactory rangeFactory2 = new RangeFactory(schema2);
        List<Range> ranges2 = new ArrayList<>();
        ranges2.add(rangeFactory2.createRange(field21, new byte[]{0, 1}, true, new byte[]{99}, false));
        ranges2.add(rangeFactory2.createRange(field22, new byte[]{100, 99}, true, new byte[]{101, 99, 99}, false));
        Region region2 = new Region(ranges2);
        Partition partition2 = Partition.builder()
                .id("id1")
                .region(region2)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        Field field31 = new Field("key1", new ByteArrayType());
        Field field32 = new Field("key2", new ByteArrayType());
        Schema schema3 = Schema.builder().rowKeyFields(field31, field32).build();
        RangeFactory rangeFactory3 = new RangeFactory(schema3);
        List<Range> ranges3 = new ArrayList<>();
        ranges3.add(rangeFactory3.createRange(field31, new byte[]{0, 1}, true, new byte[]{99}, false));
        ranges3.add(rangeFactory3.createRange(field32, new byte[]{100, 99}, true, new byte[]{101, 99, 99}, false));
        Region region3 = new Region(ranges3);
        Partition partition3 = Partition.builder()
                .id("id1")
                .region(region3)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Collections.singletonList("C1"))
                .build();

        // When
        boolean test1 = partition1.equals(partition2);
        boolean test2 = partition1.equals(partition3);
        int hashCode1 = partition1.hashCode();
        int hashCode2 = partition2.hashCode();
        int hashCode3 = partition3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfRegionNotInCanonicalForm() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition.Builder partitionBuilder = Partition.builder()
                .id("1---10")
                .region(new Region(rangeFactory.createRange(field, 1, true, 10, true)));

        // When / Then
        assertThatThrownBy(partitionBuilder::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithIntKey() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = createMidPartition(schema, 1, 10);

        // When
        boolean is1InPartition = partition.isRowKeyInPartition(schema, Key.create(1));
        boolean is5InPartition = partition.isRowKeyInPartition(schema, Key.create(5));
        boolean is10InPartition = partition.isRowKeyInPartition(schema, Key.create(10));

        // Then
        assertThat(is1InPartition).isTrue();
        assertThat(is5InPartition).isTrue();
        assertThat(is10InPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithIntKeyAndNullMax() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, 1, true, null, false);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .id("1---null")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When 1
        boolean is1InPartition = partition.isRowKeyInPartition(schema, Key.create(1));
        boolean is5InPartition = partition.isRowKeyInPartition(schema, Key.create(5));
        boolean isMinus10InPartition = partition.isRowKeyInPartition(schema, Key.create(-10));
        boolean isMaxIntInPartition = partition.isRowKeyInPartition(schema, Key.create(Integer.MAX_VALUE));

        // Then 1
        assertThat(is1InPartition).isTrue();
        assertThat(is5InPartition).isTrue();
        assertThat(isMinus10InPartition).isFalse();
        assertThat(isMaxIntInPartition).isTrue();

        // When 2
        range = rangeFactory.createRange(field, Integer.MIN_VALUE, true, null, false);
        region = new Region(range);
        partition = partition.toBuilder().region(region).build();
        boolean isIntMinValueInPartition = partition.isRowKeyInPartition(schema, Key.create(Integer.MIN_VALUE));

        // Then 2
        assertThat(isIntMinValueInPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithLongKey() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = createMidPartition(schema, 1L, 10L);

        // When
        boolean is1InPartition = partition.isRowKeyInPartition(schema, Key.create(1L));
        boolean is5InPartition = partition.isRowKeyInPartition(schema, Key.create(5L));
        boolean is10InPartition = partition.isRowKeyInPartition(schema, Key.create(10L));

        // Then
        assertThat(is1InPartition).isTrue();
        assertThat(is5InPartition).isTrue();
        assertThat(is10InPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithLongKeyAndNullMax() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, 1L, true, null, false);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .id("1---10")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When 1
        boolean is1InPartition = partition.isRowKeyInPartition(schema, Key.create(1L));
        boolean is5InPartition = partition.isRowKeyInPartition(schema, Key.create(5L));
        boolean isMinus10InPartition = partition.isRowKeyInPartition(schema, Key.create(-10L));
        boolean isMaxLongInPartition = partition.isRowKeyInPartition(schema, Key.create(Long.MAX_VALUE));

        // Then 1
        assertThat(is1InPartition).isTrue();
        assertThat(is5InPartition).isTrue();
        assertThat(isMinus10InPartition).isFalse();
        assertThat(isMaxLongInPartition).isTrue();

        // When 2
        range = rangeFactory.createRange(field, Long.MIN_VALUE, true, null, false);
        region = new Region(range);
        partition = partition.toBuilder().region(region).build();
        boolean isLongMinValueInPartition = partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(isLongMinValueInPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithStringKey() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = createMidPartition(schema, "A", "D");

        // When
        boolean isAInPartition = partition.isRowKeyInPartition(schema, Key.create("A"));
        boolean isCInPartition = partition.isRowKeyInPartition(schema, Key.create("C"));
        boolean isDInPartition = partition.isRowKeyInPartition(schema, Key.create("D"));

        // Then
        assertThat(isAInPartition).isTrue();
        assertThat(isCInPartition).isTrue();
        assertThat(isDInPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithStringKeyAndEmptyStringAndNullBoundaries() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, "", true, null, false);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .id("---null")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean isAInPartition = partition.isRowKeyInPartition(schema, Key.create("A"));
        boolean isCInPartition = partition.isRowKeyInPartition(schema, Key.create("Cqwertyuiop"));
        boolean isEmptyStringInPartition = partition.isRowKeyInPartition(schema, Key.create(""));

        // Then
        assertThat(isAInPartition).isTrue();
        assertThat(isCInPartition).isTrue();
        assertThat(isEmptyStringInPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithStringKeyAndNullBoundary() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, "G", true, null, false);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .id("---null")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean isAInPartition = partition.isRowKeyInPartition(schema, Key.create("A"));
        boolean isGInPartition = partition.isRowKeyInPartition(schema, Key.create("G"));
        boolean isGBlahInPartition = partition.isRowKeyInPartition(schema, Key.create("Gblah"));
        boolean isEmptyStringInPartition = partition.isRowKeyInPartition(schema, Key.create(""));

        // Then
        assertThat(isAInPartition).isFalse();
        assertThat(isGInPartition).isTrue();
        assertThat(isGBlahInPartition).isTrue();
        assertThat(isEmptyStringInPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithByteArrayKeyAndEmptyByteArrayAndNullBoundaries() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, new byte[]{}, true, null, false);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .id("---null")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean is1011InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{10, 11}));
        boolean isEmptyByteArrayInPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{}));

        // Then
        assertThat(is1011InPartition).isTrue();
        assertThat(isEmptyByteArrayInPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithByteArrayKeyAndNullBoundary() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createRange(field, new byte[]{10, 11}, true, null, false);
        Region region = new Region(range);
        Partition partition = Partition.builder()
                .id("---null")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean is1011InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{10, 11}));
        boolean is10110InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{10, 11, 0}));
        boolean is99InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{9, 9}));

        // Then
        assertThat(is1011InPartition).isTrue();
        assertThat(is10110InPartition).isTrue();
        assertThat(is99InPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithByteArrayKey() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = createMidPartition(schema, new byte[]{1, 64}, new byte[]{1, 88});

        // When
        boolean is164InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{1, 64}));
        boolean is170InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{1, 70}));
        boolean is188InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{1, 88}));
        boolean is288InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{2, 88}));
        boolean is16464InPartition = partition.isRowKeyInPartition(schema, Key.create(new byte[]{1, 64, 64}));

        // Then
        assertThat(is164InPartition).isTrue();
        assertThat(is170InPartition).isTrue();
        assertThat(is188InPartition).isFalse();
        assertThat(is288InPartition).isFalse();
        assertThat(is16464InPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWith2IntRowKeys() {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, 1, true, 2, false);
        Range range2 = rangeFactory.createRange(field2, 5, true, 10, false);
        Region region = new Region(Arrays.asList(range1, range2));
        Partition partition = Partition.builder()
                .id("1---10")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean is15InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(1, 5)));
        boolean is1MInPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(1, Integer.MAX_VALUE)));
        boolean is20InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(2, 0)));
        boolean is210InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(2, 10)));
        boolean is3m5InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(3, -5)));

        // Then
        assertThat(is15InPartition).isTrue();
        assertThat(is1MInPartition).isFalse();
        assertThat(is20InPartition).isFalse();
        assertThat(is210InPartition).isFalse();
        assertThat(is3m5InPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWith2StringRowKeys() {
        // Given
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, "A", true, "B", false);
        Range range2 = rangeFactory.createRange(field2, "A", true, "B", false);
        Region region = new Region(Arrays.asList(range1, range2));
        Partition partition = Partition.builder()
                .id("1---10")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean isAAInPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList("A", "A")));
        boolean isACInPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList("A", "C")));
        boolean isA123456AInPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList("A123456", "A")));
        boolean isBBInPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList("B", "B")));
        boolean isZZInPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList("Z", "Z")));

        // Then
        assertThat(isAAInPartition).isTrue();
        assertThat(isACInPartition).isFalse();
        assertThat(isA123456AInPartition).isTrue();
        assertThat(isBBInPartition).isFalse();
        assertThat(isZZInPartition).isFalse();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWith2ByteArrayRowKeys() {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, new byte[]{1, 2}, true, new byte[]{5}, false);
        Range range2 = rangeFactory.createRange(field2, new byte[]{10, 11, 12}, true, new byte[]{12, 12, 12}, false);
        Region region = new Region(Arrays.asList(range1, range2));
        Partition partition = Partition.builder()
                .id("1---10")
                .region(region)
                .leafPartition(true)
                .parentPartitionId("P")
                .childPartitionIds(Arrays.asList("C1", "C2"))
                .build();

        // When
        boolean is12101112InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{1, 2}, new byte[]{10, 11, 12})));
        boolean is1210111213InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{1, 2}, new byte[]{10, 11, 12, 13})));
        boolean is3101112InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{3}, new byte[]{10, 11, 12})));
        boolean is456121InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{4, 5, 6}, new byte[]{12, 1})));
        boolean is4888888888812121110987InPartition = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{4, 88, 88, 88, 88, 88}, new byte[]{12, 12, 11, 10, 9, 8, 7})));
        boolean is5512121110987 = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{5, 5}, new byte[]{12, 12, 11, 10, 9, 8, 7})));
        boolean is00 = partition.isRowKeyInPartition(schema, Key.create(Arrays.asList(new byte[]{0}, new byte[]{0})));

        // Then
        assertThat(is12101112InPartition).isTrue();
        assertThat(is1210111213InPartition).isTrue();
        assertThat(is3101112InPartition).isTrue();
        assertThat(is456121InPartition).isTrue();
        assertThat(is4888888888812121110987InPartition).isTrue();
        assertThat(is5512121110987).isFalse();
        assertThat(is00).isFalse();
    }
}
