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
import sleeper.core.record.SleeperRow;
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
        List<SleeperRow> records = getData1();
        Iterator<SleeperRow> iterator = records.iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema1());

        // When
        Iterator<SleeperRow> aggregated = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        SleeperRow expectedRecord1 = new SleeperRow();
        expectedRecord1.put("id", "1");
        expectedRecord1.put("count", 6L);
        SleeperRow expectedRecord2 = new SleeperRow();
        expectedRecord2.put("id", "2");
        expectedRecord2.put("count", 10L);
        SleeperRow expectedRecord3 = new SleeperRow();
        expectedRecord3.put("id", "3");
        expectedRecord3.put("count", 1100L);
        SleeperRow expectedRecord4 = new SleeperRow();
        expectedRecord4.put("id", "4");
        expectedRecord4.put("count", 1000000L);
        assertThat(aggregated).toIterable().containsExactly(
                expectedRecord1, expectedRecord2, expectedRecord3, expectedRecord4);
    }

    @Test
    public void shouldAddValuesWithByteArrayKey() {
        // Given
        List<SleeperRow> records = getData2();
        Iterator<SleeperRow> iterator = records.iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema2());

        // When
        Iterator<SleeperRow> aggregated = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        SleeperRow expectedRecord1 = new SleeperRow();
        expectedRecord1.put("id", new byte[]{1});
        expectedRecord1.put("count", 6L);
        SleeperRow expectedRecord2 = new SleeperRow();
        expectedRecord2.put("id", new byte[]{2, 2});
        expectedRecord2.put("count", 10L);
        SleeperRow expectedRecord3 = new SleeperRow();
        expectedRecord3.put("id", new byte[]{3, 1, 1});
        expectedRecord3.put("count", 1100L);
        SleeperRow expectedRecord4 = new SleeperRow();
        expectedRecord4.put("id", new byte[]{4});
        expectedRecord4.put("count", 1000000L);
        assertThat(aggregated).toIterable().containsExactly(
                expectedRecord1, expectedRecord2, expectedRecord3, expectedRecord4);
    }

    @Test
    public void shouldOutputNoRecordsIfNoRecordsInInput() {
        // Given
        Iterator<SleeperRow> iterator = List.<SleeperRow>of().iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema2());

        // When
        Iterator<SleeperRow> aggregated = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(aggregated).toIterable().isEmpty();
    }

    private static Schema getSchema1() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new StringType()))
                .valueFields(new Field("count", new LongType()))
                .build();
    }

    private static List<SleeperRow> getData1() {
        List<SleeperRow> records = new ArrayList<>();
        SleeperRow record1 = new SleeperRow();
        record1.put("id", "1");
        record1.put("count", 1L);
        records.add(record1);
        SleeperRow record2 = new SleeperRow();
        record2.put("id", "1");
        record2.put("count", 2L);
        records.add(record2);
        SleeperRow record3 = new SleeperRow();
        record3.put("id", "1");
        record3.put("count", 3L);
        records.add(record3);
        SleeperRow record4 = new SleeperRow();
        record4.put("id", "2");
        record4.put("count", 10L);
        records.add(record4);
        SleeperRow record5 = new SleeperRow();
        record5.put("id", "3");
        record5.put("count", 100L);
        records.add(record5);
        SleeperRow record6 = new SleeperRow();
        record6.put("id", "3");
        record6.put("count", 1000L);
        records.add(record6);
        SleeperRow record7 = new SleeperRow();
        record7.put("id", "4");
        record7.put("count", 1000000L);
        records.add(record7);
        return records;
    }

    private static Schema getSchema2() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new ByteArrayType()))
                .valueFields(new Field("count", new LongType()))
                .build();
    }

    private static List<SleeperRow> getData2() {
        List<SleeperRow> records = new ArrayList<>();
        SleeperRow record1 = new SleeperRow();
        record1.put("id", new byte[]{1});
        record1.put("count", 1L);
        records.add(record1);
        SleeperRow record2 = new SleeperRow();
        record2.put("id", new byte[]{1});
        record2.put("count", 2L);
        records.add(record2);
        SleeperRow record3 = new SleeperRow();
        record3.put("id", new byte[]{1});
        record3.put("count", 3L);
        records.add(record3);
        SleeperRow record4 = new SleeperRow();
        record4.put("id", new byte[]{2, 2});
        record4.put("count", 10L);
        records.add(record4);
        SleeperRow record5 = new SleeperRow();
        record5.put("id", new byte[]{3, 1, 1});
        record5.put("count", 100L);
        records.add(record5);
        SleeperRow record6 = new SleeperRow();
        record6.put("id", new byte[]{3, 1, 1});
        record6.put("count", 1000L);
        records.add(record6);
        SleeperRow record7 = new SleeperRow();
        record7.put("id", new byte[]{4});
        record7.put("count", 1000000L);
        records.add(record7);
        return records;
    }
}
