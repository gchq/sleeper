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
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SecurityFilteringIteratorTest {

    @Test
    public void shouldFilter() {
        // Given
        List<Row> rows = getData();
        Iterator<Row> iterator = rows.iterator();
        SecurityFilteringIterator securityFilteringIterator = new SecurityFilteringIterator();
        securityFilteringIterator.init("securityLabel,public", getSchema());

        // When
        Iterator<Row> filtered = securityFilteringIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(filtered).toIterable()
                .containsExactly(rows.get(0), rows.get(2));
    }

    @Test
    public void shouldAllowRowsWithEmptyVisibilitiesEvenIfNoAuths() {
        // Given
        List<Row> rows = getData();
        Iterator<Row> iterator = rows.iterator();
        SecurityFilteringIterator securityFilteringIterator = new SecurityFilteringIterator();
        securityFilteringIterator.init("securityLabel", getSchema());

        // When
        Iterator<Row> filtered = securityFilteringIterator.apply(new WrappedIterator<>(iterator));

        // Then
        assertThat(filtered).toIterable()
                .containsExactly(rows.get(2));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("field1", new StringType()))
                .valueFields(
                        new Field("field2", new LongType()),
                        new Field("securityLabel", new StringType()))
                .build();
    }

    private static List<Row> getData() {
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.put("field1", "1");
        row1.put("field2", 1000L);
        row1.put("securityLabel", "public");
        rows.add(row1);
        Row row2 = new Row();
        row2.put("field1", "2");
        row2.put("field2", 10000L);
        row2.put("securityLabel", "private");
        rows.add(row2);
        Row row3 = new Row();
        row3.put("field1", "2");
        row3.put("field2", 100000L);
        row3.put("securityLabel", "");
        rows.add(row3);
        return rows;
    }
}
