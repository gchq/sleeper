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
package sleeper.core.row;

import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;

public class RowComparatorTest {

    @Test
    public void shouldCompareCorrectlyWithIntRowKeyAndNoSortKeys() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        Row row1 = new Row();
        row1.put("key", 1);
        row1.put("value", 100);
        Row row2 = new Row();
        row2.put("key", 2);
        row2.put("value", 1000);
        Row row3 = new Row();
        row3.put("key", 1);
        row3.put("value", 10000);
        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row2, row3);
        int comparison4 = comparator.compare(row1, row1);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
        assertThat(comparison4).isZero();
    }

    @Test
    public void shouldCompareCorrectlyWithLongRowKeyAndNoSortKeys() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        Row row1 = new Row();
        row1.put("key", 1L);
        row1.put("value", 100L);
        Row row2 = new Row();
        row2.put("key", 2L);
        row2.put("value", 1000L);
        Row row3 = new Row();
        row3.put("key", 1L);
        row3.put("value", 10000L);
        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row2, row3);
        int comparison4 = comparator.compare(row1, row1);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
        assertThat(comparison4).isZero();
    }

    @Test
    public void shouldCompareCorrectlyWithStringRowKeyAndNoSortKeys() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
        Row row1 = new Row();
        row1.put("key", "1");
        row1.put("value", "100");
        Row row2 = new Row();
        row2.put("key", "2");
        row2.put("value", "1000");
        Row row3 = new Row();
        row3.put("key", "1");
        row3.put("value", "10000");
        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row2, row3);
        int comparison4 = comparator.compare(row1, row1);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
        assertThat(comparison4).isZero();
    }

    @Test
    public void shouldCompareCorrectlyWithByteArrayRowKeyAndNoSortKeys() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .valueFields(new Field("value", new ByteArrayType()))
                .build();
        Row row1 = new Row();
        row1.put("key", new byte[]{1});
        row1.put("value", new byte[]{10});
        Row row2 = new Row();
        row2.put("key", new byte[]{2});
        row2.put("value", new byte[]{100});
        Row row3 = new Row();
        row3.put("key", new byte[]{1});
        row3.put("value", new byte[]{102});
        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row2, row3);
        int comparison4 = comparator.compare(row1, row1);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
        assertThat(comparison4).isZero();
    }

    @Test
    public void shouldCompareCorrectlyWithIntRowKeyAndIntSortKey() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        Row row1 = new Row();
        row1.put("key", 1);
        row1.put("sort", 2);
        row1.put("value", 100);
        Row row2 = new Row();
        row2.put("key", 1);
        row2.put("sort", 5);
        row2.put("value", 1000);
        Row row3 = new Row();
        row3.put("key", 1);
        row3.put("sort", 1);
        row3.put("value", 10000);
        Row row4 = new Row();
        row4.put("key", 2);
        row4.put("sort", 1);
        row4.put("value", 1000);
        Row row5 = new Row();
        row5.put("key", 2);
        row5.put("sort", 1);
        row5.put("value", 1000);
        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row2, row4);
        int comparison4 = comparator.compare(row4, row5);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isGreaterThan(0);
        assertThat(comparison3).isLessThan(0);
        assertThat(comparison4).isZero();
    }

    @Test
    public void shouldCompareCorrectlyWithByteArrayRowKeyAndByteArraySortKey() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .sortKeyFields(new Field("sort", new ByteArrayType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        Row row1 = new Row();
        row1.put("key", new byte[]{1, 2});
        row1.put("sort", new byte[]{50, 51});
        row1.put("value", 100);
        Row row2 = new Row();
        row2.put("key", new byte[]{1, 3});
        row2.put("sort", new byte[]{50, 51});
        row2.put("value", 1000);
        Row row3 = new Row();
        row3.put("key", new byte[]{1, 2});
        row3.put("sort", new byte[]{50, 52});
        row3.put("value", 10000);
        Row row4 = new Row();
        row4.put("key", new byte[]{0});
        row4.put("sort", new byte[]{50, 51});
        row4.put("value", 1000);
        Row row5 = new Row();
        row5.put("key", new byte[]{0});
        row5.put("sort", new byte[]{50, 51});
        row5.put("value", 1000);
        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row2, row4);
        int comparison4 = comparator.compare(row4, row5);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isLessThan(0);
        assertThat(comparison3).isGreaterThan(0);
        assertThat(comparison4).isZero();
    }

    @Test
    public void shouldCompareCorrectlyWithMultidimensionalByteArrayRowKeyAndMultidimensionalByteArraySortKey() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()))
                .sortKeyFields(new Field("sort1", new ByteArrayType()), new Field("sort2", new ByteArrayType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        Row row1 = new Row();
        row1.put("key1", new byte[]{1, 2});
        row1.put("key2", new byte[]{1, 2});
        row1.put("sort1", new byte[]{50, 51});
        row1.put("sort2", new byte[]{50, 51});
        row1.put("value", 100);
        Row row2 = new Row();
        row2.put("key1", new byte[]{1, 3});
        row2.put("key2", new byte[]{1, 2});
        row2.put("sort1", new byte[]{50, 51});
        row2.put("sort2", new byte[]{50, 51});
        row2.put("value", 1000);
        Row row3 = new Row();
        row3.put("key1", new byte[]{1, 2});
        row3.put("key2", new byte[]{1, 3});
        row3.put("sort1", new byte[]{50, 51});
        row3.put("sort2", new byte[]{50, 51});
        row3.put("value", 100);
        Row row4 = new Row();
        row4.put("key1", new byte[]{1, 2});
        row4.put("key2", new byte[]{1, 2});
        row4.put("sort1", new byte[]{50, 52});
        row4.put("sort2", new byte[]{50, 51});
        row4.put("value", 1000);
        Row row5 = new Row();
        row5.put("key1", new byte[]{1, 2});
        row5.put("key2", new byte[]{1, 2});
        row5.put("sort1", new byte[]{50, 51});
        row5.put("sort2", new byte[]{50, 52});
        row5.put("value", 1000);
        Row row6 = new Row();
        row6.put("key1", new byte[]{1, 2});
        row6.put("key2", new byte[]{1, 3});
        row6.put("sort1", new byte[]{50, 52});
        row6.put("sort2", new byte[]{50, 51});
        row6.put("value", 10000);
        Row row7 = new Row();
        row7.put("key1", new byte[]{0});
        row7.put("key2", new byte[]{1, 2});
        row7.put("sort1", new byte[]{50, 51});
        row7.put("sort2", new byte[]{50, 51});
        row7.put("value", 1000);
        Row row8 = new Row();
        row8.put("key1", new byte[]{0});
        row8.put("key2", new byte[]{1, 2});
        row8.put("sort1", new byte[]{50, 51});
        row8.put("sort2", new byte[]{50, 51});
        row8.put("value", 1000);

        RowComparator comparator = new RowComparator(schema);

        // When
        int comparison1 = comparator.compare(row1, row2);
        int comparison2 = comparator.compare(row1, row3);
        int comparison3 = comparator.compare(row1, row4);
        int comparison4 = comparator.compare(row1, row5);
        int comparison5 = comparator.compare(row1, row6);
        int comparison6 = comparator.compare(row2, row7);
        int comparison7 = comparator.compare(row7, row8);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isLessThan(0);
        assertThat(comparison3).isLessThan(0);
        assertThat(comparison4).isLessThan(0);
        assertThat(comparison5).isLessThan(0);
        assertThat(comparison6).isGreaterThan(0);
        assertThat(comparison7).isZero();
    }
}
