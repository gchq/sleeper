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
package sleeper.core.record.testutils;

import org.junit.jupiter.api.Test;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SortedRecordsCheckTest {

    @Test
    void shouldFindRecordsAreSortedWithDifferentValues() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 10L)),
                new Record(Map.of("key", 20L)),
                new Record(Map.of("key", 30L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(SortedRecordsCheck.sorted(3));
    }

    @Test
    void shouldFindFirstTwoRecordsAreOutOfOrder() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 20L)),
                new Record(Map.of("key", 10L)),
                new Record(Map.of("key", 30L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(
                SortedRecordsCheck.outOfOrderAt(2,
                        new Record(Map.of("key", 20L)),
                        new Record(Map.of("key", 10L))));
    }

    @Test
    void shouldFindLastTwoRecordsAreOutOfOrder() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 10L)),
                new Record(Map.of("key", 30L)),
                new Record(Map.of("key", 20L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(
                SortedRecordsCheck.outOfOrderAt(3,
                        new Record(Map.of("key", 30L)),
                        new Record(Map.of("key", 20L))));
    }

    @Test
    void shouldFindRecordsAreSortedWithSameValue() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 20L)),
                new Record(Map.of("key", 20L)),
                new Record(Map.of("key", 20L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(SortedRecordsCheck.sorted(3));
    }

    @Test
    void shouldFindRecordsAreOutOfOrderBySortKey() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("row", new LongType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .build();
        List<Record> records = List.of(
                new Record(Map.of("row", 10L, "sort", 10L)),
                new Record(Map.of("row", 10L, "sort", 30L)),
                new Record(Map.of("row", 10L, "sort", 20L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(
                SortedRecordsCheck.outOfOrderAt(3,
                        new Record(Map.of("row", 10L, "sort", 30L)),
                        new Record(Map.of("row", 10L, "sort", 20L))));
    }

    @Test
    void shouldFindOneRecordIsSorted() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of(
                new Record(Map.of("key", 10L)));

        // When / Then
        assertThat(check(schema, records)).isEqualTo(SortedRecordsCheck.sorted(1));
    }

    @Test
    void shouldFindNoRecordsAreSorted() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Record> records = List.of();

        // When / Then
        assertThat(check(schema, records)).isEqualTo(SortedRecordsCheck.sorted(0));
    }

    private SortedRecordsCheck check(Schema schema, List<Record> records) {
        return SortedRecordsCheck.check(schema, records.iterator());
    }

}
