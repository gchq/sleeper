/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.core.record.Record;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class FilteringIteratorTest {

    @Test
    void shouldFilterIterator() {
        // Given
        Record record1 = new Record(Map.of("key", "A"));
        Record record2 = new Record(Map.of("key", "B"));
        CloseableIterator<Record> iterator = new WrappedIterator<>(List.of(record1, record2).iterator());

        // When
        FilteringIterator<Record> filtered = new FilteringIterator<>(iterator,
                record -> "A".equals(record.get("key")));

        // Then
        assertThat(filtered).toIterable().containsExactly(record1);
        assertThat(iterator).isExhausted();
    }

    @Test
    void shouldCloseSourceIterator() throws Exception {
        // Given
        AtomicBoolean closed = new AtomicBoolean(false);
        EmptyIteratorWithFakeOnClose iterator = new EmptyIteratorWithFakeOnClose(() -> closed.set(true));

        // When
        new FilteringIterator<>(iterator, record -> true).close();

        // Then
        assertThat(closed).isTrue();
    }
}
