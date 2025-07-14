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
import sleeper.core.row.Row;
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
        Row row1 = new Row();
        row1.put("key", 1);
        row1.put("sort", 1);
        row1.put("value", 1);
        Row row2 = new Row();
        row2.put("key", 1);
        row2.put("sort", 2);
        row2.put("value", -1);
        Row row3 = new Row();
        row3.put("key", 3);
        row3.put("sort", 1);
        row3.put("value", 3);
        list1.add(row1);
        list1.add(row2);
        list1.add(row3);
        List<Row> list2 = new ArrayList<>();
        Row row4 = new Row();
        row4.put("key", 1);
        row4.put("sort", 1);
        row4.put("value", 1);
        Row row5 = new Row();
        row5.put("key", 2);
        row5.put("sort", 1000000);
        row5.put("value", 1);
        Row row6 = new Row();
        row6.put("key", 4);
        row6.put("sort", 1000000);
        row6.put("value", 4);
        Row row7 = new Row();
        row7.put("key", 5);
        row7.put("sort", 1000000);
        row7.put("value", 6);
        list2.add(row4);
        list2.add(row5);
        list2.add(row6);
        list2.add(row7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                row1, row4, row2, row5, row3, row6, row7);
        assertThat(mergingIterator.getNumberOfRowsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyLongKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new LongType(), new LongType(), new StringType());
        List<Row> list1 = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", 1L);
        row1.put("sort", 1L);
        row1.put("value", "1");
        Row row2 = new Row();
        row2.put("key", 1L);
        row2.put("sort", 2L);
        row2.put("value", "-1");
        Row row3 = new Row();
        row3.put("key", 3L);
        row3.put("sort", 1L);
        row3.put("value", "3");
        list1.add(row1);
        list1.add(row2);
        list1.add(row3);
        List<Row> list2 = new ArrayList<>();
        Row row4 = new Row();
        row4.put("key", 1L);
        row4.put("sort", 1L);
        row4.put("value", "1");
        Row row5 = new Row();
        row5.put("key", 2L);
        row5.put("sort", 1000000L);
        row5.put("value", "1");
        Row row6 = new Row();
        row6.put("key", 4L);
        row6.put("sort", 1000000L);
        row6.put("value", "4");
        Row row7 = new Row();
        row7.put("key", 5);
        row7.put("sort", 1000000L);
        row7.put("value", "6");
        list2.add(row4);
        list2.add(row5);
        list2.add(row6);
        list2.add(row7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                row1, row4, row2, row5, row3, row6, row7);
        assertThat(mergingIterator.getNumberOfRowsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyStringKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new StringType(), new LongType(), new StringType());
        List<Row> list1 = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", "A");
        row1.put("sort", 1L);
        row1.put("value", "1");
        Row row2 = new Row();
        row2.put("key", "A");
        row2.put("sort", 2L);
        row2.put("value", "-1");
        Row row3 = new Row();
        row3.put("key", "C");
        row3.put("sort", 1L);
        row3.put("value", "3");
        list1.add(row1);
        list1.add(row2);
        list1.add(row3);
        List<Row> list2 = new ArrayList<>();
        Row row4 = new Row();
        row4.put("key", "A");
        row4.put("sort", 1L);
        row4.put("value", "1");
        Row row5 = new Row();
        row5.put("key", "B");
        row5.put("sort", 1000000L);
        row5.put("value", "1");
        Row row6 = new Row();
        row6.put("key", "D");
        row6.put("sort", 1000000L);
        row6.put("value", "4");
        Row row7 = new Row();
        row7.put("key", "E");
        row7.put("sort", 1000000L);
        row7.put("value", "6");
        list2.add(row4);
        list2.add(row5);
        list2.add(row6);
        list2.add(row7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                row1, row4, row2, row5, row3, row6, row7);
        assertThat(mergingIterator.getNumberOfRowsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyByteArrayKey() {
        // Given
        Schema schema = schemaWithKeySortAndValueTypes(new ByteArrayType(), new LongType(), new StringType());
        List<Row> list1 = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", new byte[]{1});
        row1.put("sort", 1L);
        row1.put("value", "1");
        Row row2 = new Row();
        row2.put("key", new byte[]{1});
        row2.put("sort", 2L);
        row2.put("value", "-1");
        Row row3 = new Row();
        row3.put("key", new byte[]{3});
        row3.put("sort", 1L);
        row3.put("value", "3");
        list1.add(row1);
        list1.add(row2);
        list1.add(row3);
        List<Row> list2 = new ArrayList<>();
        Row row4 = new Row();
        row4.put("key", new byte[]{1});
        row4.put("sort", 1L);
        row4.put("value", "1");
        Row row5 = new Row();
        row5.put("key", new byte[]{2});
        row5.put("sort", 1000000L);
        row5.put("value", "1");
        Row row6 = new Row();
        row6.put("key", new byte[]{4, 4});
        row6.put("sort", 1000000L);
        row6.put("value", "4");
        Row row7 = new Row();
        row7.put("key", new byte[]{5});
        row7.put("sort", 1000000L);
        row7.put("value", "6");
        list2.add(row4);
        list2.add(row5);
        list2.add(row6);
        list2.add(row7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                row1, row4, row2, row5, row3, row6, row7);
        assertThat(mergingIterator.getNumberOfRowsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyWhenNoSortKey() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        List<Row> list1 = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", 1);
        row1.put("value", 1);
        Row row2 = new Row();
        row2.put("key", 1);
        row2.put("value", 1);
        Row row3 = new Row();
        row3.put("key", 3);
        row3.put("value", 3);
        list1.add(row1);
        list1.add(row2);
        list1.add(row3);
        List<Row> list2 = new ArrayList<>();
        Row row4 = new Row();
        row4.put("key", 1);
        row4.put("value", 1);
        Row row5 = new Row();
        row5.put("key", 2);
        row5.put("value", 1);
        Row row6 = new Row();
        row6.put("key", 4);
        row6.put("value", 4);
        Row row7 = new Row();
        row7.put("key", 5);
        row7.put("value", 6);
        list2.add(row4);
        list2.add(row5);
        list2.add(row6);
        list2.add(row7);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(list2.iterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                row1, row2, row4, row5, row3, row6, row7);
        assertThat(mergingIterator.getNumberOfRowsRead()).isEqualTo(7L);
    }

    @Test
    public void shouldMergeSortedIterablesCorrectlyWhenOneIsEmpty() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
        List<Row> list1 = new ArrayList<>();
        Row row1 = new Row();
        row1.put("key", 1);
        row1.put("value", 1);
        Row row2 = new Row();
        row2.put("key", 1);
        row2.put("value", 1);
        Row row3 = new Row();
        row3.put("key", 3);
        row3.put("value", 3);
        list1.add(row1);
        list1.add(row2);
        list1.add(row3);

        // When
        CloseableIterator<Row> iterator1 = new WrappedIterator<>(list1.iterator());
        CloseableIterator<Row> iterator2 = new WrappedIterator<>(Collections.emptyIterator());
        MergingIterator mergingIterator = new MergingIterator(schema, Arrays.asList(iterator1, iterator2));

        // Then
        assertThat(mergingIterator).toIterable().containsExactly(
                row1, row2, row3);
        assertThat(mergingIterator.getNumberOfRowsRead()).isEqualTo(3L);
    }
}
