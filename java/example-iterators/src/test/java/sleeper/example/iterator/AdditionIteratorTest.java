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
package sleeper.example.iterator;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
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
        List<Record> records = getData1();
        Iterator<Record> iterator = records.iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema1());

        // When
        Iterator<Record> filtered = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        Record expectedRecord1 = new Record();
        expectedRecord1.put("id", "1");
        expectedRecord1.put("count", 6L);
        Record expectedRecord2 = new Record();
        expectedRecord2.put("id", "2");
        expectedRecord2.put("count", 10L);
        Record expectedRecord3 = new Record();
        expectedRecord3.put("id", "3");
        expectedRecord3.put("count", 1100L);
        assertThat(filtered).toIterable().containsExactly(
                expectedRecord1, expectedRecord2, expectedRecord3);
    }

    @Test
    public void shouldAddValuesWithByteArrayKey() {
        // Given
        List<Record> records = getData2();
        Iterator<Record> iterator = records.iterator();
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", getSchema2());

        // When
        Iterator<Record> filtered = additionIterator.apply(new WrappedIterator<>(iterator));

        // Then
        Record expectedRecord1 = new Record();
        expectedRecord1.put("id", new byte[]{1});
        expectedRecord1.put("count", 6L);
        Record expectedRecord2 = new Record();
        expectedRecord2.put("id", new byte[]{2, 2});
        expectedRecord2.put("count", 10L);
        Record expectedRecord3 = new Record();
        expectedRecord3.put("id", new byte[]{3, 1, 1});
        expectedRecord3.put("count", 1100L);
        assertThat(filtered).toIterable().containsExactly(
                expectedRecord1, expectedRecord2, expectedRecord3);
    }

    private static Schema getSchema1() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new StringType()))
                .valueFields(new Field("count", new LongType()))
                .build();
    }

    private static List<Record> getData1() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("id", "1");
        record1.put("count", 1L);
        records.add(record1);
        Record record2 = new Record();
        record2.put("id", "1");
        record2.put("count", 2L);
        records.add(record2);
        Record record3 = new Record();
        record3.put("id", "1");
        record3.put("count", 3L);
        records.add(record3);
        Record record4 = new Record();
        record4.put("id", "2");
        record4.put("count", 10L);
        records.add(record4);
        Record record5 = new Record();
        record5.put("id", "3");
        record5.put("count", 100L);
        records.add(record5);
        Record record6 = new Record();
        record6.put("id", "3");
        record6.put("count", 1000L);
        records.add(record6);
        return records;
    }

    private static Schema getSchema2() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new ByteArrayType()))
                .valueFields(new Field("count", new LongType()))
                .build();
    }

    private static List<Record> getData2() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("id", new byte[]{1});
        record1.put("count", 1L);
        records.add(record1);
        Record record2 = new Record();
        record2.put("id", new byte[]{1});
        record2.put("count", 2L);
        records.add(record2);
        Record record3 = new Record();
        record3.put("id", new byte[]{1});
        record3.put("count", 3L);
        records.add(record3);
        Record record4 = new Record();
        record4.put("id", new byte[]{2, 2});
        record4.put("count", 10L);
        records.add(record4);
        Record record5 = new Record();
        record5.put("id", new byte[]{3, 1, 1});
        record5.put("count", 100L);
        records.add(record5);
        Record record6 = new Record();
        record6.put("id", new byte[]{3, 1, 1});
        record6.put("count", 1000L);
        records.add(record6);
        return records;
    }
}
