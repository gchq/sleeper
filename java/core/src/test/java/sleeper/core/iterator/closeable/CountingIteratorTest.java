/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.iterator.closeable;

import org.junit.jupiter.api.Test;

import sleeper.core.iterator.testutil.EmptyIteratorWithFakeOnClose;
import sleeper.core.row.Row;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class CountingIteratorTest {

    @Test
    void shouldFindNoItems() {
        // Given
        CountingIterator<String> iterator = new CountingIterator<>(iteratorOver(List.of()));

        // When / Then
        assertThat(gatherToList(iterator)).isEmpty();
        assertThat(iterator.getCount()).isZero();
    }

    @Test
    void shouldFindSomeItems() {
        // Given
        CountingIterator<String> iterator = new CountingIterator<>(iteratorOver(List.of("a", "b", "c")));

        // When / Then
        assertThat(gatherToList(iterator)).containsExactly("a", "b", "c");
        assertThat(iterator.getCount()).isEqualTo(3);
    }

    @Test
    void shouldCloseSourceIterator() {
        // Given
        AtomicBoolean closed = new AtomicBoolean(false);
        EmptyIteratorWithFakeOnClose source = new EmptyIteratorWithFakeOnClose(() -> closed.set(true));
        CountingIterator<Row> iterator = new CountingIterator<>(source);

        // When / Then
        assertThat(gatherToList(iterator)).isEmpty();
        assertThat(closed).isTrue();
    }

    private static <T> CloseableIterator<T> iteratorOver(List<T> list) {
        return new WrappedIterator<>(list.iterator());
    }

    private static <T> List<T> gatherToList(CountingIterator<T> iterator) {
        try (iterator) {
            List<T> list = new ArrayList<>();
            iterator.forEachRemaining(list::add);
            return list;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
