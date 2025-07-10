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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FixedAgeOffIteratorTest {

    Schema schema = Schema.builder()
            .rowKeyFields(new Field("id", new StringType()))
            .valueFields(new Field("timestamp", new LongType()))
            .build();

    @Test
    public void shouldAgeOff() {
        // Given
        List<Row> rows = rows(List.of(
                Map.of("id", "1", "timestamp", 1000L),
                Map.of("id", "2", "timestamp", 1300L),
                Map.of("id", "3", "timestamp", 1500L),
                Map.of("id", "4", "timestamp", 1800L)));
        FixedAgeOffIterator ageOffIterator = new FixedAgeOffIterator();
        ageOffIterator.init("timestamp,1500", schema);

        // When
        Iterator<Row> filtered = ageOffIterator.apply(new WrappedIterator<>(rows.iterator()));

        // Then
        assertThat(filtered).toIterable()
                .extracting(row -> row.getValues(List.of("id", "timestamp")))
                .containsExactly(
                        List.of("3", 1500L),
                        List.of("4", 1800L));
    }

    private static List<Row> rows(List<Map<String, Object>> rows) {
        return rows.stream().map(map -> new Row(map)).toList();
    }

}
