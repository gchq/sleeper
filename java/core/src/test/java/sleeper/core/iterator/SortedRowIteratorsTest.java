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
import static sleeper.core.iterator.SortedRowIteratorTestHelper.apply;

public class SortedRowIteratorsTest {

    @Test
    void shouldApplyMultipleIteratorsInSequence() {
        // Given
        SortedRowIterators iterators = new SortedRowIterators(List.of(
                filterOnValueField("key", 2),
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

    protected static SortedRowIterator filterOnValueField(String field, Object value) {
        return withRequiredValueFields(List.of(field),
                input -> new FilteringIterator<>(input, row -> Objects.equals(value, row.get(field))));
    }

    protected static SortedRowIterator limitRows(int limit) {
        return withNoRequiredValueFields(input -> new LimitingIterator<>(limit, input));
    }

    protected static SortedRowIterator withRequiredValueFields(List<String> requiredValueFields) {
        return withRequiredValueFields(requiredValueFields, input -> input);
    }

    protected static SortedRowIterator withNoRequiredValueFields(Function<CloseableIterator<Row>, CloseableIterator<Row>> apply) {
        return withRequiredValueFields(List.of(), apply);
    }

    private static SortedRowIterator withRequiredValueFields(List<String> requiredValueFields, Function<CloseableIterator<Row>, CloseableIterator<Row>> apply) {
        return new SortedRowIterator() {
            @Override
            public CloseableIterator<Row> apply(CloseableIterator<Row> input) {
                return apply.apply(input);
            }

            @Override
            public List<String> getRequiredValueFields() {
                return requiredValueFields;
            }
        };
    }
}
