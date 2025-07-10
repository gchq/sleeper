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
package sleeper.core.iterator;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
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
        List<Row> rows = getData();
        Iterator<Row> iterator = rows.iterator();
        AgeOffIterator ageOffIterator = new AgeOffIterator();
        ageOffIterator.init("timestamp,1000000", getSchema());

        // When
        Iterator<Row> filtered = ageOffIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(filtered).toIterable()
                .containsExactly(rows.get(1), rows.get(4));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("id", new StringType()))
                .valueFields(new Field("timestamp", new LongType()))
                .build();
    }

    private static List<Row> getData() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("id", "1");
        row1.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        rows.add(row1);
        Row row2 = new Row();
        row2.put("id", "1");
        row2.put("timestamp", System.currentTimeMillis());
        rows.add(row2);
        Row row3 = new Row();
        row3.put("id", "2");
        row3.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        rows.add(row3);
        Row row4 = new Row();
        row4.put("id", "3");
        row4.put("timestamp", System.currentTimeMillis() - 2_000_000L);
        rows.add(row4);
        Row row5 = new Row();
        row5.put("id", "4");
        row5.put("timestamp", System.currentTimeMillis());
        rows.add(row5);
        return rows;
    }
}
