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
package sleeper.example.iterator;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.WrappedIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AdditionIteratorTest {

    @Test
    public void shouldAddValues() {
        // Given
        List<Row> rows = getData1();
        Iterator<Row> iterator = rows.iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema1());

        // When
        Iterator<Row> aggregated = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        Row expectedRow1 = new Row();
        expectedRow1.put("id", "1");
        expectedRow1.put("count", 6L);
        Row expectedRow2 = new Row();
        expectedRow2.put("id", "2");
        expectedRow2.put("count", 10L);
        Row expectedRow3 = new Row();
        expectedRow3.put("id", "3");
        expectedRow3.put("count", 1100L);
        Row expectedRow4 = new Row();
        expectedRow4.put("id", "4");
        expectedRow4.put("count", 1000000L);
        assertThat(aggregated).toIterable().containsExactly(
                expectedRow1, expectedRow2, expectedRow3, expectedRow4);
    }

    @Test
    public void shouldAddValuesWithByteArrayKey() {
        // Given
        List<Row> rows = getData2();
        Iterator<Row> iterator = rows.iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema2());

        // When
        Iterator<Row> aggregated = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        Row expectedRow1 = new Row();
        expectedRow1.put("id", new byte[]{1});
        expectedRow1.put("count", 6L);
        Row expectedRow2 = new Row();
        expectedRow2.put("id", new byte[]{2, 2});
        expectedRow2.put("count", 10L);
        Row expectedRow3 = new Row();
        expectedRow3.put("id", new byte[]{3, 1, 1});
        expectedRow3.put("count", 1100L);
        Row expectedRow4 = new Row();
        expectedRow4.put("id", new byte[]{4});
        expectedRow4.put("count", 1000000L);
        assertThat(aggregated).toIterable().containsExactly(
                expectedRow1, expectedRow2, expectedRow3, expectedRow4);
    }

    @Test
    public void shouldOutputNoRecordsIfNoRowsInInput() {
        // Given
        Iterator<Row> iterator = List.<Row>of().iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema2());

        // When
        Iterator<Row> aggregated = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(aggregated).toIterable().isEmpty();
    }

    private static Schema getSchema1() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new StringType()))
                .valueFields(new Field("count", new LongType()))
                .build();
    }

    private static List<Row> getData1() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("id", "1");
        row1.put("count", 1L);
        rows.add(row1);
        Row row2 = new Row();
        row2.put("id", "1");
        row2.put("count", 2L);
        rows.add(row2);
        Row row3 = new Row();
        row3.put("id", "1");
        row3.put("count", 3L);
        rows.add(row3);
        Row row4 = new Row();
        row4.put("id", "2");
        row4.put("count", 10L);
        rows.add(row4);
        Row row5 = new Row();
        row5.put("id", "3");
        row5.put("count", 100L);
        rows.add(row5);
        Row row6 = new Row();
        row6.put("id", "3");
        row6.put("count", 1000L);
        rows.add(row6);
        Row row7 = new Row();
        row7.put("id", "4");
        row7.put("count", 1000000L);
        rows.add(row7);
        return rows;
    }

    private static Schema getSchema2() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new ByteArrayType()))
                .valueFields(new Field("count", new LongType()))
                .build();
    }

    private static List<Row> getData2() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("id", new byte[]{1});
        row1.put("count", 1L);
        rows.add(row1);
        Row row2 = new Row();
        row2.put("id", new byte[]{1});
        row2.put("count", 2L);
        rows.add(row2);
        Row row3 = new Row();
        row3.put("id", new byte[]{1});
        row3.put("count", 3L);
        rows.add(row3);
        Row row4 = new Row();
        row4.put("id", new byte[]{2, 2});
        row4.put("count", 10L);
        rows.add(row4);
        Row row5 = new Row();
        row5.put("id", new byte[]{3, 1, 1});
        row5.put("count", 100L);
        rows.add(row5);
        Row row6 = new Row();
        row6.put("id", new byte[]{3, 1, 1});
        row6.put("count", 1000L);
        rows.add(row6);
        Row row7 = new Row();
        row7.put("id", new byte[]{4});
        row7.put("count", 1000000L);
        rows.add(row7);
        return rows;
    }
}
