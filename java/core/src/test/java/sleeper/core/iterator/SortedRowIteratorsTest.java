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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.iterator.SortedRowIteratorTestHelper.apply;
import static sleeper.core.iterator.SortedRowIteratorTestHelper.filterOnValueField;
import static sleeper.core.iterator.SortedRowIteratorTestHelper.limitRows;
import static sleeper.core.iterator.SortedRowIteratorTestHelper.withRequiredValueFields;

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
}
