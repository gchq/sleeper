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
package sleeper.core;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MergingIteratorTest {

    private Schema schemaWithKeySortAndValueTypes(PrimitiveType keyType, PrimitiveType sortType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", keyType))
                .sortKeyFields(new Field("sort", sortType))
                .valueFields(new Field("value", valueType))
                .build();
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyIntKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new IntType(), new IntType(), new IntType());
        List<Row> list1 = new ArrayList<>();
        Row record1 = new Row();
        record1.put("key", 1);
        record1.put("sort", 1);
        record1.put("value", 1);
        Row record2 = new Row();
        record2.put("key", 1);
        record2.put("sort", 2);
        record2.put("value", -1);
        Row record3 = new Row();
        record3.put("key", 3);
        record3.put("sort", 1);
        record3.put("value", 3);
        list1.add(record1);
        list1.add(record2);
        list1.add(record3);
        List<Row> list2 = new ArrayList<>();
        Row record4 = new Row();
        record4.put("key", 1);
        record4.put("sort", 1);
        record4.put("value", 1);
        Row record5 = new Row();
        record5.put("key", 2);
        record5.put("sort", 1000000);
        record5.put("value", 1);
        Row record6 = new Row();
        record6.put("key", 4);
        record6.put("sort", 1000000);
        record6.put("value", 4);
        Row record7 = new Row();
        record7.put("key", 5);
        record7.put("sort", 1000000);
        record7.put("value", 6);
        list2.add(record4);
        list2.add(record5);
        list2.add(record6);
        list2.add(record7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                record1, record4, record2, record5, record3, record6, record7);
        assertThat(mergingIterator.getNumberOfRecordsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyLongKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new LongType(), new LongType(), new StringType());
        List<Row> list1 = new ArrayList<>();
        Row record1 = new Row();
        record1.put("key", 1L);
        record1.put("sort", 1L);
        record1.put("value", "1");
        Row record2 = new Row();
        record2.put("key", 1L);
        record2.put("sort", 2L);
        record2.put("value", "-1");
        Row record3 = new Row();
        record3.put("key", 3L);
        record3.put("sort", 1L);
        record3.put("value", "3");
        list1.add(record1);
        list1.add(record2);
        list1.add(record3);
        List<Row> list2 = new ArrayList<>();
        Row record4 = new Row();
        record4.put("key", 1L);
        record4.put("sort", 1L);
        record4.put("value", "1");
        Row record5 = new Row();
        record5.put("key", 2L);
        record5.put("sort", 1000000L);
        record5.put("value", "1");
        Row record6 = new Row();
        record6.put("key", 4L);
        record6.put("sort", 1000000L);
        record6.put("value", "4");
        Row record7 = new Row();
        record7.put("key", 5);
        record7.put("sort", 1000000L);
        record7.put("value", "6");
        list2.add(record4);
        list2.add(record5);
        list2.add(record6);
        list2.add(record7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                record1, record4, record2, record5, record3, record6, record7);
        assertThat(mergingIterator.getNumberOfRecordsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyStringKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new StringType(), new LongType(), new StringType());
        List<Row> list1 = new ArrayList<>();
        Row record1 = new Row();
        record1.put("key", "A");
        record1.put("sort", 1L);
        record1.put("value", "1");
        Row record2 = new Row();
        record2.put("key", "A");
        record2.put("sort", 2L);
        record2.put("value", "-1");
        Row record3 = new Row();
        record3.put("key", "C");
        record3.put("sort", 1L);
        record3.put("value", "3");
        list1.add(record1);
        list1.add(record2);
        list1.add(record3);
        List<Row> list2 = new ArrayList<>();
        Row record4 = new Row();
        record4.put("key", "A");
        record4.put("sort", 1L);
        record4.put("value", "1");
        Row record5 = new Row();
        record5.put("key", "B");
        record5.put("sort", 1000000L);
        record5.put("value", "1");
        Row record6 = new Row();
        record6.put("key", "D");
        record6.put("sort", 1000000L);
        record6.put("value", "4");
        Row record7 = new Row();
        record7.put("key", "E");
        record7.put("sort", 1000000L);
        record7.put("value", "6");
        list2.add(record4);
        list2.add(record5);
        list2.add(record6);
        list2.add(record7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                record1, record4, record2, record5, record3, record6, record7);
        assertThat(mergingIterator.getNumberOfRecordsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyByteArrayKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new ByteArrayType(), new LongType(), new StringType());
        List<Row> list1 = new ArrayList<>();
        Row record1 = new Row();
        record1.put("key", new byte[]{1});
        record1.put("sort", 1L);
        record1.put("value", "1");
        Row record2 = new Row();
        record2.put("key", new byte[]{1});
        record2.put("sort", 2L);
        record2.put("value", "-1");
        Row record3 = new Row();
        record3.put("key", new byte[]{3});
        record3.put("sort", 1L);
        record3.put("value", "3");
        list1.add(record1);
        list1.add(record2);
        list1.add(record3);
        List<Row> list2 = new ArrayList<>();
        Row record4 = new Row();
        record4.put("key", new byte[]{1});
        record4.put("sort", 1L);
        record4.put("value", "1");
        Row record5 = new Row();
        record5.put("key", new byte[]{2});
        record5.put("sort", 1000000L);
        record5.put("value", "1");
        Row record6 = new Row();
        record6.put("key", new byte[]{4, 4});
        record6.put("sort", 1000000L);
        record6.put("value", "4");
        Row record7 = new Row();
        record7.put("key", new byte[]{5});
        record7.put("sort", 1000000L);
        record7.put("value", "6");
        list2.add(record4);
        list2.add(record5);
        list2.add(record6);
        list2.add(record7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                record1, record4, record2, record5, record3, record6, record7);
        assertThat(mergingIterator.getNumberOfRecordsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyWhenNoSortKey() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        List<Row> list1 = new ArrayList<>();
        Row record1 = new Row();
        record1.put("key", 1);
        record1.put("value", 1);
        Row record2 = new Row();
        record2.put("key", 1);
        record2.put("value", 1);
        Row record3 = new Row();
        record3.put("key", 3);
        record3.put("value", 3);
        list1.add(record1);
        list1.add(record2);
        list1.add(record3);
        List<Row> list2 = new ArrayList<>();
        Row record4 = new Row();
        record4.put("key", 1);
        record4.put("value", 1);
        Row record5 = new Row();
        record5.put("key", 2);
        record5.put("value", 1);
        Row record6 = new Row();
        record6.put("key", 4);
        record6.put("value", 4);
        Row record7 = new Row();
        record7.put("key", 5);
        record7.put("value", 6);
        list2.add(record4);
        list2.add(record5);
        list2.add(record6);
        list2.add(record7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                record1, record2, record4, record5, record3, record6, record7);
        assertThat(mergingIterator.getNumberOfRecordsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyWhenOneIsEmpty() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        List<Row> list1 = new ArrayList<>();
        Row record1 = new Row();
        record1.put("key", 1);
        record1.put("value", 1);
        Row record2 = new Row();
        record2.put("key", 1);
        record2.put("value", 1);
        Row record3 = new Row();
        record3.put("key", 3);
        record3.put("value", 3);
        list1.add(record1);
        list1.add(record2);
        list1.add(record3);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(Collections.emptyIterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                record1, record2, record3);
        assertThat(mergingIterator.getNumberOfRecordsRead()).isEqualTo(3L);
    }
}
