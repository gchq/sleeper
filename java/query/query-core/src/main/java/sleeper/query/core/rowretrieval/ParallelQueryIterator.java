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
package sleeper.query.core.rowretrieval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Iterates over rows produced by leaf partition query tasks running in a thread pool. Each task reads all rows from
 * its partition and feeds them into a shared bounded queue; this iterator drains from that queue. Whichever partition
 * produces rows fastest is consumed first — a slow partition does not block rows from faster ones. The queue provides
 * backpressure: producers block when it is full until the consumer catches up.
 */
class ParallelQueryIterator implements CloseableIterator<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelQueryIterator.class);

    private final BlockingQueue<QueueItem> queue;
    private final List<Future<?>> futures;
    private final AtomicBoolean closed;
    private int remainingProducers;
    private Row nextRow = null;

    ParallelQueryIterator(BlockingQueue<QueueItem> queue, List<Future<?>> futures, AtomicBoolean closed) {
        this.queue = queue;
        this.futures = futures;
        this.closed = closed;
        this.remainingProducers = futures.size();
        LOGGER.info("Initialised ParallelQueryIterator with {} producers", this.remainingProducers);
    }

    @Override
    public boolean hasNext() {
        while (nextRow == null) {
            if (remainingProducers == 0) {
                return false;
            }
            QueueItem item;
            try {
                item = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (item.isDone()) {
                remainingProducers--;
            } else if (item.isError()) {
                remainingProducers--;
                throw item.error;
            } else {
                nextRow = item.row;
            }
        }
        return true;
    }

    @Override
    public Row next() {
        if (nextRow == null) {
            throw new NoSuchElementException();
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }

    @Override
    public void close() {
        closed.set(true);
        queue.clear();
        futures.forEach(f -> f.cancel(true));
    }

    /**
     * An item placed in the shared queue by a producer thread. Represents either a row, a done signal, or an error.
     */
    static class QueueItem {
        static final QueueItem DONE = new QueueItem(null, null);

        final Row row;
        final RuntimeException error;

        private QueueItem(Row row, RuntimeException error) {
            this.row = row;
            this.error = error;
        }

        static QueueItem row(Row r) {
            return new QueueItem(r, null);
        }

        static QueueItem error(RuntimeException e) {
            return new QueueItem(null, e);
        }

        public boolean isDone() {
            return this == DONE;
        }

        public boolean isError() {
            return error != null;
        }
    }
}
