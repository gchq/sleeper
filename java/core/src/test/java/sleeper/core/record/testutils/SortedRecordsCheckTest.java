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

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    @Test
    void shouldCloseIterator() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        AtomicBoolean closed = new AtomicBoolean(false);
        OnCloseIterator iterator = new OnCloseIterator(() -> closed.set(true));

        // When
        SortedRecordsCheck.check(schema, iterator);

        // Then
        assertThat(closed).isTrue();
    }

    @Test
    void shouldWrapIteratorCloseIOException() {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        IOException failure = new IOException("Unexpected failure");
        OnCloseIterator iterator = new OnCloseIterator(() -> {
            throw failure;
        });

        // When / Then
        assertThatThrownBy(() -> SortedRecordsCheck.check(schema, iterator))
                .isInstanceOf(UncheckedIOException.class)
                .hasCause(failure);
    }

    private SortedRecordsCheck check(Schema schema, List<Record> records) {
        return SortedRecordsCheck.check(schema, new WrappedIterator<>(records.iterator()));
    }

    private static class OnCloseIterator implements CloseableIterator<Record> {

        private final OnClose onClose;

        OnCloseIterator(OnClose onClose) {
            this.onClose = onClose;
        }

        @Override
        public void close() throws IOException {
            onClose.close();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Record next() {
            throw new UnsupportedOperationException("Unimplemented method 'next'");
        }
    }

    private interface OnClose {
        void close() throws IOException;
    }

}
