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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import sleeper.core.util.ThreadSleep;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProgressTrackingIteratorTest {

    private final List<Thread> threads = new ArrayList<>();
    private final List<Long> progressReports = new ArrayList<>();
    private final Semaphore pollRequested = new Semaphore(0);
    private final Semaphore pollCompleted = new Semaphore(0);
    private Runnable onSourceClose = () -> {
    };

    @AfterEach
    void tearDown() throws InterruptedException {
        for (Thread thread : threads) {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    void shouldFindNoItems() {
        // Given
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(1, List.of());

        // When / Then
        assertThat(gatherToList(iterator)).isEmpty();
        assertThat(progressReports).containsExactly(0L);
        assertThat(iterator.getCount()).isZero();
    }

    @Test
    void shouldReportEvery3ItemsAndAtEnd() {
        // Given
        List<String> items = List.of("a", "b", "c", "d", "e", "f", "g");
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(3, items);

        // When / Then
        assertThat(gatherToList(iterator)).containsExactlyElementsOf(items);
        assertThat(progressReports).containsExactly(3L, 6L, 7L);
        assertThat(iterator.getCount()).isEqualTo(7);
    }

    @Test
    void shouldReportOnce() {
        // Given
        List<String> items = List.of("a", "b", "c");
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(3, items);

        // When / Then
        assertThat(gatherToList(iterator)).containsExactlyElementsOf(items);
        assertThat(progressReports).containsExactly(3L);
        assertThat(iterator.getCount()).isEqualTo(3);
    }

    @Test
    void shouldStopPollingThreadOnClose() {
        // Given
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(3, List.of("a", "b"));

        // When
        gatherToList(iterator);

        // Then
        assertThat(threads)
                .singleElement()
                .extracting(Thread::isAlive)
                .isEqualTo(false);
    }

    @Test
    void shouldStopPollingThreadWhenSourceFailsToClose() {
        // Given
        onSourceClose = () -> {
            throw new RuntimeException("Could not close source");
        };
        ProgressTrackingIterator<String> iterator = trackProgressEveryNItems(3, List.of("a", "b"));

        // When / Then
        assertThatThrownBy(() -> gatherToList(iterator))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Could not close source");
        assertThat(threads)
                .singleElement()
                .extracting(Thread::isAlive)
                .isEqualTo(false);
    }

    private ProgressTrackingIterator<String> trackProgressEveryNItems(int n, List<String> items) {
        CloseableIterator<String> source = new IteratorWaitingOnPoll<>(items, n);
        LongConsumer callback = this::trackProgressAndReleasePollCompleted;
        ThreadSleep threadSleep = this::waitUntilTimeToPollProgress;
        ThreadFactory threadFactory = this::createAndTrackThread;
        ProgressTrackingIterator<String> iterator = new ProgressTrackingIterator<String>(
                source, callback, Duration.ofSeconds(1),
                threadSleep, threadFactory);
        iterator.startProgressPolling();
        return iterator;
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

    private void trackProgressAndReleasePollCompleted(long count) {
        progressReports.add(count);
        pollCompleted.release();
    }

    private void waitUntilTimeToPollProgress(long millis) throws InterruptedException {
        pollRequested.acquire();
    }

    private Thread createAndTrackThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        threads.add(thread);
        return thread;
    }

    class IteratorWaitingOnPoll<T> implements CloseableIterator<T> {
        private final List<T> items;
        private final long waitEveryNItems;
        private int read = 0;

        IteratorWaitingOnPoll(List<T> items, long waitEveryNItems) {
            this.items = items;
            this.waitEveryNItems = waitEveryNItems;
        }

        @Override
        public boolean hasNext() {
            return read < items.size();
        }

        @Override
        public T next() {
            if (read > 0 && read % waitEveryNItems == 0) {
                pollRequested.release();
                pollCompleted.acquireUninterruptibly();
            }
            return items.get(read++);
        }

        @Override
        public void close() {
            onSourceClose.run();
        }
    }

}
