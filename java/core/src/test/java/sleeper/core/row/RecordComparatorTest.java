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

public class RecordComparatorTest {

    @Test
    public void shouldCompareCorrectlyWithIntRowKeyAndNoSortKeys() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        Record record1 = new Record();
        record1.put("key", 1);
        record1.put("value", 100);
        Record record2 = new Record();
        record2.put("key", 2);
        record2.put("value", 1000);
        Record record3 = new Record();
        record3.put("key", 1);
        record3.put("value", 10000);
        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record2, record3);
        int comparison4 = comparator.compare(record1, record1);

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
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value", 100L);
        Record record2 = new Record();
        record2.put("key", 2L);
        record2.put("value", 1000L);
        Record record3 = new Record();
        record3.put("key", 1L);
        record3.put("value", 10000L);
        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record2, record3);
        int comparison4 = comparator.compare(record1, record1);

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
        Record record1 = new Record();
        record1.put("key", "1");
        record1.put("value", "100");
        Record record2 = new Record();
        record2.put("key", "2");
        record2.put("value", "1000");
        Record record3 = new Record();
        record3.put("key", "1");
        record3.put("value", "10000");
        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record2, record3);
        int comparison4 = comparator.compare(record1, record1);

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
        Record record1 = new Record();
        record1.put("key", new byte[]{1});
        record1.put("value", new byte[]{10});
        Record record2 = new Record();
        record2.put("key", new byte[]{2});
        record2.put("value", new byte[]{100});
        Record record3 = new Record();
        record3.put("key", new byte[]{1});
        record3.put("value", new byte[]{102});
        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record2, record3);
        int comparison4 = comparator.compare(record1, record1);

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
        Record record1 = new Record();
        record1.put("key", 1);
        record1.put("sort", 2);
        record1.put("value", 100);
        Record record2 = new Record();
        record2.put("key", 1);
        record2.put("sort", 5);
        record2.put("value", 1000);
        Record record3 = new Record();
        record3.put("key", 1);
        record3.put("sort", 1);
        record3.put("value", 10000);
        Record record4 = new Record();
        record4.put("key", 2);
        record4.put("sort", 1);
        record4.put("value", 1000);
        Record record5 = new Record();
        record5.put("key", 2);
        record5.put("sort", 1);
        record5.put("value", 1000);
        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record2, record4);
        int comparison4 = comparator.compare(record4, record5);

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
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 2});
        record1.put("sort", new byte[]{50, 51});
        record1.put("value", 100);
        Record record2 = new Record();
        record2.put("key", new byte[]{1, 3});
        record2.put("sort", new byte[]{50, 51});
        record2.put("value", 1000);
        Record record3 = new Record();
        record3.put("key", new byte[]{1, 2});
        record3.put("sort", new byte[]{50, 52});
        record3.put("value", 10000);
        Record record4 = new Record();
        record4.put("key", new byte[]{0});
        record4.put("sort", new byte[]{50, 51});
        record4.put("value", 1000);
        Record record5 = new Record();
        record5.put("key", new byte[]{0});
        record5.put("sort", new byte[]{50, 51});
        record5.put("value", 1000);
        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record2, record4);
        int comparison4 = comparator.compare(record4, record5);

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
        Record record1 = new Record();
        record1.put("key1", new byte[]{1, 2});
        record1.put("key2", new byte[]{1, 2});
        record1.put("sort1", new byte[]{50, 51});
        record1.put("sort2", new byte[]{50, 51});
        record1.put("value", 100);
        Record record2 = new Record();
        record2.put("key1", new byte[]{1, 3});
        record2.put("key2", new byte[]{1, 2});
        record2.put("sort1", new byte[]{50, 51});
        record2.put("sort2", new byte[]{50, 51});
        record2.put("value", 1000);
        Record record3 = new Record();
        record3.put("key1", new byte[]{1, 2});
        record3.put("key2", new byte[]{1, 3});
        record3.put("sort1", new byte[]{50, 51});
        record3.put("sort2", new byte[]{50, 51});
        record3.put("value", 100);
        Record record4 = new Record();
        record4.put("key1", new byte[]{1, 2});
        record4.put("key2", new byte[]{1, 2});
        record4.put("sort1", new byte[]{50, 52});
        record4.put("sort2", new byte[]{50, 51});
        record4.put("value", 1000);
        Record record5 = new Record();
        record5.put("key1", new byte[]{1, 2});
        record5.put("key2", new byte[]{1, 2});
        record5.put("sort1", new byte[]{50, 51});
        record5.put("sort2", new byte[]{50, 52});
        record5.put("value", 1000);
        Record record6 = new Record();
        record6.put("key1", new byte[]{1, 2});
        record6.put("key2", new byte[]{1, 3});
        record6.put("sort1", new byte[]{50, 52});
        record6.put("sort2", new byte[]{50, 51});
        record6.put("value", 10000);
        Record record7 = new Record();
        record7.put("key1", new byte[]{0});
        record7.put("key2", new byte[]{1, 2});
        record7.put("sort1", new byte[]{50, 51});
        record7.put("sort2", new byte[]{50, 51});
        record7.put("value", 1000);
        Record record8 = new Record();
        record8.put("key1", new byte[]{0});
        record8.put("key2", new byte[]{1, 2});
        record8.put("sort1", new byte[]{50, 51});
        record8.put("sort2", new byte[]{50, 51});
        record8.put("value", 1000);

        RecordComparator comparator = new RecordComparator(schema);

        // When
        int comparison1 = comparator.compare(record1, record2);
        int comparison2 = comparator.compare(record1, record3);
        int comparison3 = comparator.compare(record1, record4);
        int comparison4 = comparator.compare(record1, record5);
        int comparison5 = comparator.compare(record1, record6);
        int comparison6 = comparator.compare(record2, record7);
        int comparison7 = comparator.compare(record7, record8);

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
