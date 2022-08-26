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
package sleeper.core.partition;

import org.junit.Test;
import sleeper.core.key.Key;
import sleeper.core.range.Range;
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

    @Test
    public void testEqualsAndHashcodeWithIntKeyWithNulls() {
        // Given
        Field field1 = new Field("key", new IntType());
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new IntType());
        partition1.setId("0---100");
        Region region1 = new Region(new Range(field1, 0, true, null, false));
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId("P");
        partition1.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Field field2 = new Field("key", new IntType());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new IntType());
        partition2.setId("0---100");
        Region region2 = new Region(new Range(field2, 0, true, null, false));
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId("P");
        partition2.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Partition partition3 = new Partition();
        partition3.setRowKeyTypes(new IntType());
        partition3.setId("0---100");
        Region region3 = new Region(new Range(field2, 0, true, 100, false));
        partition3.setRegion(region3);
        partition3.setLeafPartition(true);
        partition3.setParentPartitionId("P");
        partition3.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new LongType());
        partition1.setId("0---100");
        Region region1 = new Region(new Range(field1, 0L, true, 100L, false));
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId("P");
        partition1.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Field field2 = new Field("key", new LongType());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new LongType());
        partition2.setId("0---100");
        Region region2 = new Region(new Range(field2, 0L, true, 100L, false));
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId("P");
        partition2.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Partition partition3 = new Partition();
        partition3.setRowKeyTypes(new LongType());
        partition3.setId("0---100");
        Region region3 = new Region(new Range(field2, 0L, true, 100L, false));
        partition3.setRegion(region3);
        partition3.setLeafPartition(true);
        partition3.setParentPartitionId("P");
        partition3.setChildPartitionIds(Collections.singletonList("C1"));

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
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new LongType());
        partition1.setId("0---100");
        Region region1 = new Region(new Range(field1, 0L, true, 100L, false));
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId("P");
        partition1.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Field field2 = new Field("key", new LongType());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new LongType());
        partition2.setId("0---100");
        Region region2 = new Region(new Range(field2, 0L, true, 100L, false));
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId("P");
        partition2.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Partition partition3 = new Partition();
        partition3.setRowKeyTypes(new ByteArrayType());
        partition3.setId("0---100");
        Region region3 = new Region(new Range(field1, 0L, true, 100L, false));
        partition3.setRegion(region3);

        partition3.setLeafPartition(true);
        partition3.setParentPartitionId("P");
        partition3.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new ByteArrayType());
        partition1.setId("0---100");
        Region region1 = new Region(new Range(field1, new byte[]{(byte) 0}, true, new byte[]{(byte) 100}, false));
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId("P");
        partition1.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Schema schema2 = new Schema();
        Field field2 = new Field("key", new ByteArrayType());
        schema2.setRowKeyFields(field2);
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new ByteArrayType());
        partition2.setId("0---100");
        Region region2 = new Region(new Range(field2, new byte[]{(byte) 0}, true, new byte[]{(byte) 100}, false));
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId("P");
        partition2.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Partition partition3 = new Partition();
        partition3.setRowKeyTypes(new ByteArrayType());
        partition3.setId("0---100");
        Region region3 = new Region(new Range(field1, new byte[]{(byte) 0}, true, new byte[]{(byte) 100}, false));
        partition3.setRegion(region3);
        partition3.setLeafPartition(true);
        partition3.setParentPartitionId("P");
        partition3.setChildPartitionIds(Collections.singletonList("C1"));

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
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        partition1.setId("id1");
        List<Range> ranges1 = new ArrayList<>();
        ranges1.add(new Range(field11, new byte[]{0, 1}, true, new byte[]{99}, false));
        ranges1.add(new Range(field12, new byte[]{100, 99}, true, new byte[]{101, 99, 99}, false));
        Region region1 = new Region(ranges1);
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId("P");
        partition1.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Field field21 = new Field("key1", new ByteArrayType());
        Field field22 = new Field("key2", new ByteArrayType());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        partition2.setId("id1");
        List<Range> ranges2 = new ArrayList<>();
        ranges2.add(new Range(field21, new byte[]{0, 1}, true, new byte[]{99}, false));
        ranges2.add(new Range(field22, new byte[]{100, 99}, true, new byte[]{101, 99, 99}, false));
        Region region2 = new Region(ranges2);
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId("P");
        partition2.setChildPartitionIds(Arrays.asList("C1", "C2"));
        Partition partition3 = new Partition();
        partition3.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        partition3.setId("id1");
        List<Range> ranges3 = new ArrayList<>();
        ranges3.add(new Range(field21, new byte[]{0, 1}, true, new byte[]{99}, false));
        ranges3.add(new Range(field22, new byte[]{100, 99}, true, new byte[]{101, 99, 99}, false));
        Region region3 = new Region(ranges3);
        partition3.setRegion(region3);
        partition3.setLeafPartition(true);
        partition3.setParentPartitionId("P");
        partition3.setChildPartitionIds(Collections.singletonList("C1"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new IntType());
        partition.setId("1---10");
        Region region = new Region(new Range(field, 1, true, 10, true));

        // When / Then
        assertThatThrownBy(() -> partition.setRegion(region))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithIntKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new IntType());
        partition.setId("1---10");
        Range range = new Range(field, 1, true, 10, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new IntType());
        partition.setId("1---null");
        Range range = new Range(field, 1, true, null, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        range = new Range(field, Integer.MIN_VALUE, true, null, false);
        region = new Region(range);
        partition.setRegion(region);
        boolean isIntMinValueInPartition = partition.isRowKeyInPartition(schema, Key.create(Integer.MIN_VALUE));

        // Then 2
        assertThat(isIntMinValueInPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithLongKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new LongType());
        partition.setId("1---10");
        Range range = new Range(field, 1L, true, 10L, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new LongType());
        partition.setId("1---10");
        Range range = new Range(field, 1L, true, null, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        range = new Range(field, Long.MIN_VALUE, true, null, false);
        region = new Region(range);
        partition.setRegion(region);
        boolean isLongMinValueInPartition = partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(isLongMinValueInPartition).isTrue();
    }

    @Test
    public void shouldGiveCorrectAnswerForIsRowKeyInPartitionWithStringKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new StringType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new StringType());
        partition.setId("A---D");
        Range range = new Range(field, "A", true, "D", false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new StringType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new StringType());
        partition.setId("---null");
        Range range = new Range(field, "", true, null, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new StringType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new StringType());
        partition.setId("---null");
        Range range = new Range(field, "G", true, null, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setId("---null");
        Range range = new Range(field, new byte[]{}, true, null, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setId("---null");
        Range range = new Range(field, new byte[]{10, 11}, true, null, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setId("id");
        Range range = new Range(field, new byte[]{1, 64}, true, new byte[]{1, 88}, false);
        Region region = new Region(range);
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new IntType());
        schema.setRowKeyFields(field1, field2);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new IntType(), new IntType());
        partition.setId("1---10");
        Range range1 = new Range(field1, 1, true, 2, false);
        Range range2 = new Range(field2, 5, true, 10, false);
        Region region = new Region(Arrays.asList(range1, range2));
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        schema.setRowKeyFields(field1, field2);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new StringType(), new StringType());
        partition.setId("1---10");
        Range range1 = new Range(field1, "A", true, "B", false);
        Range range2 = new Range(field2, "A", true, "B", false);
        Region region = new Region(Arrays.asList(range1, range2));
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
        Schema schema = new Schema();
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        schema.setRowKeyFields(field1, field2);
        Partition partition = new Partition();
        partition.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        partition.setId("1---10");
        Range range1 = new Range(field1, new byte[]{1, 2}, true, new byte[]{5}, false);
        Range range2 = new Range(field2, new byte[]{10, 11, 12}, true, new byte[]{12, 12, 12}, false);
        Region region = new Region(Arrays.asList(range1, range2));
        partition.setRegion(region);
        partition.setLeafPartition(true);
        partition.setParentPartitionId("P");
        partition.setChildPartitionIds(Arrays.asList("C1", "C2"));

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
