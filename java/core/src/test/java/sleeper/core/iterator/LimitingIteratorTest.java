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

import sleeper.core.row.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class LimitingIteratorTest {

    @Test
    void shouldLimitValuesRead() throws Exception {
        // Given
        List<String> values = List.of("A", "B", "C");
        WrappedIterator<String> source = new WrappedIterator<>(values.iterator());

        // When
        List<String> found = new ArrayList<>();
        try (LimitingIterator<String> iterator = new LimitingIterator<>(2, source)) {
            iterator.forEachRemaining(found::add);
        }

        // Then
        assertThat(found).containsExactly("A", "B");
    }

    @Test
    void shouldReadAllValuesWhenBelowLimit() throws Exception {
        // Given
        List<String> values = List.of("A", "B", "C");
        WrappedIterator<String> source = new WrappedIterator<>(values.iterator());

        // When
        List<String> found = new ArrayList<>();
        try (LimitingIterator<String> iterator = new LimitingIterator<>(5, source)) {
            iterator.forEachRemaining(found::add);
        }

        // Then
        assertThat(found).containsExactly("A", "B", "C");
    }

    @Test
    void shouldReadNoValues() throws Exception {
        // Given
        List<String> values = List.of();
        WrappedIterator<String> source = new WrappedIterator<>(values.iterator());

        // When
        List<String> found = new ArrayList<>();
        try (LimitingIterator<String> iterator = new LimitingIterator<>(5, source)) {
            iterator.forEachRemaining(found::add);
        }

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    void shouldCloseWrappedIterator() throws Exception {
        // Given
        AtomicBoolean closed = new AtomicBoolean(false);
        EmptyIteratorWithFakeOnClose source = new EmptyIteratorWithFakeOnClose(() -> closed.set(true));
        LimitingIterator<Record> iterator = new LimitingIterator<>(10, source);

        // When
        iterator.close();

        // Then
        assertThat(closed).isTrue();
    }

}
