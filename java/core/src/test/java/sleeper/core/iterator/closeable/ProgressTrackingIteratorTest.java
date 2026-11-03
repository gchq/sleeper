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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

public class ProgressTrackingIteratorTest {

    private final List<Long> progressReports = new ArrayList<>();

    @Test
    void shouldFindNoItems() {
        // Given
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(1, iteratorOver(List.of()));

        // When / Then
        assertThat(gatherToList(iterator)).isEmpty();
        assertThat(progressReports).containsExactly(0L);
        assertThat(iterator.getCount()).isZero();
    }

    @Test
    void shouldReportEvery3ItemsAndAtEnd() {
        // Given
        List<String> items = List.of("a", "b", "c", "d", "e", "f", "g");
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(3, iteratorOver(items));

        // When / Then
        assertThat(gatherToList(iterator)).containsExactlyElementsOf(items);
        assertThat(progressReports).containsExactly(3L, 6L, 7L);
        assertThat(iterator.getCount()).isEqualTo(7);
    }

    @Test
    void shouldReportOnce() {
        // Given
        List<String> items = List.of("a", "b", "c");
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(3, iteratorOver(items));

        // When / Then
        assertThat(gatherToList(iterator)).containsExactlyElementsOf(items);
        assertThat(progressReports).containsExactly(3L);
        assertThat(iterator.getCount()).isEqualTo(3);
    }

    private <T> ProgressTrackingIterator<T> trackProgressEveryNItems(long n, CloseableIterator<T> source) {
        CountingIterator<T> counting = new CountingIterator<>(source);
        AtomicLong lastCount = new AtomicLong(0);
        return new ProgressTrackingIterator<>(counting, progressReports::add, Duration.ofSeconds(1), time -> {
            while (counting.getCount() < lastCount.get() + n) {
                Thread.sleep(1);
            }
            lastCount.set(counting.getCount());
        });
    }

    private static <T> CloseableIterator<T> iteratorOver(List<T> list) {
        return new WrappedIterator<>(list.stream().peek(item -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }).iterator());
    }

    private static <T> List<T> gatherToList(CloseableIterator<T> iterator) {
        try (iterator) {
            List<T> list = new ArrayList<>();
            iterator.forEachRemaining(list::add);
            return list;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
