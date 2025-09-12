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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.FilteringIterator;
import sleeper.core.iterator.closeable.LimitingIterator;
import sleeper.core.row.Row;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class SortedRowIteratorsTest extends SortedRowIteratorTestBase {

    @Test
    void shouldApplyMultipleIteratorsInSequence() {
        // Given
        SortedRowIterators iterators = new SortedRowIterators(List.of(
                filterAcceptingFieldValue("key", 2),
                limitRows(2)));

        // When
        List<Row> output = apply(iterators, List.of(
                new Row(Map.of("key", 1, "value", "a")),
                new Row(Map.of("key", 2, "value", "b")),
                new Row(Map.of("key", 2, "value", "c")),
                new Row(Map.of("key", 2, "value", "d"))));

        // Then
        assertThat(output).containsExactly(
                new Row(Map.of("key", 2, "value", "b")),
                new Row(Map.of("key", 2, "value", "c")));
    }

    @Test
    void shouldCombineRequiredValueFields() {
        // Given
        SortedRowIterators iterators = new SortedRowIterators(List.of(
                withRequiredValueFields(List.of("a", "b")),
                withRequiredValueFields(List.of("c", "d"))));

        // When / Then
        assertThat(iterators.getRequiredValueFields())
                .containsExactly("a", "b", "c", "d");
    }

    @Test
    void shouldRemoveDuplicateRequiredValueFields() {
        // Given
        SortedRowIterators iterators = new SortedRowIterators(List.of(
                withRequiredValueFields(List.of("something", "value", "other")),
                withRequiredValueFields(List.of("thing", "value", "else", "something"))));

        // When / Then
        assertThat(iterators.getRequiredValueFields())
                .containsExactly("something", "value", "other", "thing", "else");
    }

    private SortedRowIterator filterAcceptingFieldValue(String field, Object value) {
        return simpleIterator(input -> new FilteringIterator<>(input, row -> Objects.equals(value, row.get(field))));
    }

    private SortedRowIterator limitRows(int limit) {
        return simpleIterator(input -> new LimitingIterator<>(limit, input));
    }

    private SortedRowIterator withRequiredValueFields(List<String> requiredValueFields) {
        return new SortedRowIterator() {
            @Override
            public CloseableIterator<Row> apply(CloseableIterator<Row> input) {
                return input;
            }

            @Override
            public List<String> getRequiredValueFields() {
                return requiredValueFields;
            }
        };
    }

    private SortedRowIterator simpleIterator(Function<CloseableIterator<Row>, CloseableIterator<Row>> apply) {
        return new SortedRowIterator() {
            @Override
            public CloseableIterator<Row> apply(CloseableIterator<Row> input) {
                return apply.apply(input);
            }

            @Override
            public List<String> getRequiredValueFields() {
                return List.of();
            }
        };
    }

}
