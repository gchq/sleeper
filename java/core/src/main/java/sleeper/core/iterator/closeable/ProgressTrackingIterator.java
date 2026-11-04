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

import sleeper.core.util.ThreadSleep;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * An iterator that counts the elements from a source iterator as it goes, and regularly reports on the count. To make
 * reports to the callback with progress updates while it runs, call {@link #startProgressPolling()} before you start
 * reading from the iterator. Polling will stop when the iterator is closed.
 *
 * @param <T> the element type
 */
public class ProgressTrackingIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> source;
    private final LongConsumer callback;
    private final AtomicLong count = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Thread pollingThread;

    public ProgressTrackingIterator(
            CloseableIterator<T> source, LongConsumer callback,
            Duration pollInterval, ThreadSleep threadSleep, ThreadFactory threadFactory) {
        this.source = source;
        this.callback = callback;
        this.pollingThread = threadFactory.newThread(() -> {
            while (!closed.get()) {
                try {
                    threadSleep.waitForDuration(pollInterval);
                } catch (InterruptedException e) {
                    return;
                }
                callback.accept(count.get());
            }
        });
        pollingThread.setDaemon(true);
    }

    /**
     * Starts a background thread to provide periodic updates on the number of rows read.
     */
    public void startProgressPolling() {
        pollingThread.start();
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        T row = source.next();
        count.incrementAndGet();
        return row;
    }

    @Override
    public void close() throws IOException {
        try {
            source.close();
        } finally {
            closed.set(true);
            pollingThread.interrupt();
            try {
                pollingThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            callback.accept(count.get());
        }
    }

    /**
     * Retrieves the number of elements read from the source.
     *
     * @return the count
     */
    public long getCount() {
        return count.get();
    }

}
