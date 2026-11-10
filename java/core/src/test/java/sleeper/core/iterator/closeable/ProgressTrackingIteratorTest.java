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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.InstanceOfAssertFactories.THROWABLE;

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

    @Test
    void shouldMakeFinalReportAfterSlowPollFinishes() throws Exception {
        // Given an iterator whose polls are slow to report
        List<Long> reports = Collections.synchronizedList(new ArrayList<>());
        Semaphore pollStarted = new Semaphore(0);
        ProgressTrackingIterator<String> iterator = trackProgressWithSourceItemsAndCallback(
                List.of("a", "b"),
                delayPollReportUntilClosingThreadJoins(Thread.currentThread(), pollStarted, reports));

        // And a poll reads the count after the first item
        iterator.next();
        pollRequested.release();
        pollStarted.acquire();

        // And the second item is read before that poll reports
        iterator.next();

        // When
        iterator.close();

        // Then the poll's stale count is reported before the final count
        assertThat(reports).containsExactly(1L, 2L);
    }

    @Test
    void shouldSkipFinalReportWhenInterruptedWaitingForPollToFinish() throws Exception {
        // Given an iterator whose polls interrupt the closing thread
        List<Long> reports = Collections.synchronizedList(new ArrayList<>());
        Semaphore pollStarted = new Semaphore(0);
        ProgressTrackingIterator<String> iterator = trackProgressWithSourceItemsAndCallback(
                List.of("a", "b"),
                interruptClosingThreadWhileJoining(Thread.currentThread(), pollStarted, reports));

        // And all items are read, then a poll starts
        iterator.forEachRemaining(item -> {
        });
        pollRequested.release();
        pollStarted.acquire();

        // When
        Throwable thrown = catchThrowable(iterator::close);
        boolean interrupted = Thread.interrupted();

        // Then
        assertThat(thrown).isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(interrupted).isTrue();
        assertThat(reports).isEmpty();
    }

    @Test
    void shouldKeepSourceFailureWhenInterruptedWaitingForPollToFinish() throws Exception {
        // Given an iterator whose polls interrupt the closing thread
        List<Long> reports = Collections.synchronizedList(new ArrayList<>());
        Semaphore pollStarted = new Semaphore(0);
        ProgressTrackingIterator<String> iterator = trackProgressWithSourceItemsAndCallback(
                List.of("a", "b"),
                interruptClosingThreadWhileJoining(Thread.currentThread(), pollStarted, reports));

        // And the source fails to close
        RuntimeException sourceFailure = new RuntimeException("Could not close source");
        onSourceClose = () -> {
            throw sourceFailure;
        };

        // And all items are read, then a poll starts
        iterator.forEachRemaining(item -> {
        });
        pollRequested.release();
        pollStarted.acquire();

        // When
        Throwable thrown = catchThrowable(iterator::close);
        boolean interrupted = Thread.interrupted();

        // Then the source failure is thrown, with the interrupt suppressed
        assertThat(thrown).isSameAs(sourceFailure);
        assertThat(thrown.getSuppressed())
                .singleElement(as(THROWABLE))
                .hasCauseInstanceOf(InterruptedException.class);
        assertThat(interrupted).isTrue();
        assertThat(reports).isEmpty();
    }

    // Holds up reports from the polling thread until the iterator is closed and the closing thread is waiting to join
    // the polling thread. Without the join, the closing thread would report the final count before the poll reports.
    private static LongConsumer delayPollReportUntilClosingThreadJoins(Thread closingThread, Semaphore pollStarted, List<Long> reports) {
        return count -> {
            if (Thread.currentThread() != closingThread) {
                pollStarted.release();
                waitForInterruptFromClose();
                waitUntilJoining(closingThread);
            }
            reports.add(count);
        };
    }

    // Interrupts the closing thread while it's waiting to join the polling thread. The polling thread then keeps running
    // until the test is torn down, so the join can't finish before the closing thread sees the interrupt.
    private static LongConsumer interruptClosingThreadWhileJoining(Thread closingThread, Semaphore pollStarted, List<Long> reports) {
        return count -> {
            if (Thread.currentThread() != closingThread) {
                pollStarted.release();
                waitForInterruptFromClose();
                waitUntilJoining(closingThread);
                closingThread.interrupt();
                waitForInterruptFromTearDown();
            } else {
                reports.add(count);
            }
        };
    }

    private <T> ProgressTrackingIterator<T> trackProgressEveryNItems(int n, List<T> items) {
        return trackProgress(new IteratorWaitingOnPoll<>(items, n), this::trackProgressAndReleasePollCompleted);
    }

    private <T> ProgressTrackingIterator<T> trackProgressWithSourceItemsAndCallback(List<T> items, LongConsumer callback) {
        CloseableIterator<T> source = new WrappedIterator<>(items.iterator()) {
            @Override
            public void close() {
                onSourceClose.run();
            }
        };
        return trackProgress(source, callback);
    }

    private <T> ProgressTrackingIterator<T> trackProgress(CloseableIterator<T> source, LongConsumer callback) {
        ThreadSleep threadSleep = this::waitUntilTimeToPollProgress;
        ThreadFactory threadFactory = this::createAndTrackThread;
        ProgressTrackingIterator<T> iterator = new ProgressTrackingIterator<>(
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

    private static void waitForInterruptFromClose() {
        waitForInterrupt();
    }

    private static void waitForInterruptFromTearDown() {
        waitForInterrupt();
    }

    private static void waitForInterrupt() {
        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            // Expected
        }
    }

    private static void waitUntilJoining(Thread closingThread) {
        // After the polling thread is interrupted, the closing thread only waits when it joins the polling thread
        while (closingThread.getState() != Thread.State.WAITING) {
            Thread.onSpinWait();
        }
    }

    private Thread createAndTrackThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        threads.add(thread);
        return thread;
    }

    private class IteratorWaitingOnPoll<T> implements CloseableIterator<T> {
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
