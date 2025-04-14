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
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AgeOffIteratorTest {

    @Test
    public void shouldAgeOff() {
        // Given
        List<Record> records = getData();
        Iterator<Record> iterator = records.iterator();
        AgeOffIterator ageOffIterator = new AgeOffIterator();
        ageOffIterator.init("timestamp,1000000", getSchema());

        // When
        Iterator<Record> filtered = ageOffIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(filtered).toIterable()
                .containsExactly(records.get(1), records.get(4));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new StringType()))
                .valueFields(new Field("timestamp", new LongType()))
                .build();
    }

    private static List<Record> getData() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("id", "1");
        record1.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        records.add(record1);
        Record record2 = new Record();
        record2.put("id", "1");
        record2.put("timestamp", System.currentTimeMillis());
        records.add(record2);
        Record record3 = new Record();
        record3.put("id", "2");
        record3.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        records.add(record3);
        Record record4 = new Record();
        record4.put("id", "3");
        record4.put("timestamp", System.currentTimeMillis() - 2_000_000L);
        records.add(record4);
        Record record5 = new Record();
        record5.put("id", "4");
        record5.put("timestamp", System.currentTimeMillis());
        records.add(record5);
        return records;
    }
}
