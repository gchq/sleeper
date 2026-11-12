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
package sleeper.clients.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.api.QueryResultsReader.ResultsFile;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads query results files in parallel in a thread pool. Each file is read by a task that places its rows into a
 * shared bounded queue. Files can be submitted at any time, which allows this class to be used to read results files
 * that are being created as subqueries complete. vered. The rows are returned in no particular order, as results may
 * be interleaved from different results files. The queue provides backpressure: readers block when it is full until
 * the consumer catches up.
 * <p>
 * This mirrors the pattern in ParallelQueryIterator and QueryExecutor.executeInParallel in the query-core module,
 * which read leaf partitions in parallel during a direct query. The difference is that there the set of producers is
 * fixed upfront, whereas here files may be submitted while the consumer is already reading, as results files are
 * discovered while a query is still running. If this is changed, consider whether the same change applies there.
 * <p>
 * This class is not thread-safe for the consumer: submit files and take rows from a single thread.
 */
class ParallelResultsFileReader implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelResultsFileReader.class);
    private static final int BUFFER_SIZE = 10000;

    private final BlockingQueue<QueueItem> queue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    private final ExecutorService executorService;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<Future<?>> futures = new ArrayList<>();
    private int activeFiles;

    /**
     * Creates a reader with a thread pool created for this reader. The thread pool is shut down when this reader is
     * closed.
     *
     * @param threads the number of threads to read files in parallel
     */
    ParallelResultsFileReader(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("threads must be at least 1");
        }
        this.executorService = Executors.newFixedThreadPool(threads);
    }

    /**
     * Submits a results file to be read in the thread pool.
     *
     * @param file the results file
     */
    void submit(ResultsFile file) {
        LOGGER.info("Submitting results file {} to be read", file.name());
        futures.add(executorService.submit(() -> readFile(file)));
        activeFiles++;
    }

    /**
     * Retrieves the number of submitted files that have not yet been fully read.
     *
     * @return the number of files
     */
    int activeFiles() {
        return activeFiles;
    }

    /**
     * Takes the next row read from any of the submitted files, blocking until one is available. Returns null when all
     * submitted files have been fully read and their rows taken. If reading any file failed, the failure is thrown
     * from here once all rows read before it have been taken.
     *
     * @return the next row, or null if all submitted files have been fully read
     */
    Row takeNextRow() {
        while (activeFiles > 0) {
            QueueItem item;
            try {
                item = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for rows from results files", e);
            }
            if (item.isFileDone()) {
                activeFiles--;
            } else if (item.isError()) {
                activeFiles--;
                throw item.error;
            } else {
                return item.row;
            }
        }
        return null;
    }

    @Override
    public void close() {
        closed.set(true);
        queue.clear();
        futures.forEach(future -> future.cancel(true));
        executorService.shutdown();
    }

    private void readFile(ResultsFile file) {
        boolean sentTerminal = false;
        try (CloseableIterator<Row> rows = file.contents().get()) {
            while (rows.hasNext() && !closed.get()) {
                queue.put(QueueItem.row(rows.next()));
            }
        } catch (RuntimeException e) {
            putIfOpen(QueueItem.error(e));
            sentTerminal = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sentTerminal = true; // close() was called, FILE_DONE not needed
        } catch (IOException ignored) {
            // thrown by close() - nothing meaningful to do with a close failure here
        } finally {
            if (!sentTerminal) {
                putIfOpen(QueueItem.FILE_DONE);
            }
        }
    }

    private void putIfOpen(QueueItem item) {
        if (closed.get()) {
            return;
        }
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            // queue.put is a blocking call which can throw an InterruptedException; re-set the interrupt flag on the
            // thread so that the caller is aware.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * An item placed in the shared queue by a file reader task. Represents either a row, a signal that a file has been
     * fully read, or an error.
     */
    private static class QueueItem {
        static final QueueItem FILE_DONE = new QueueItem(null, null);

        private final Row row;
        private final RuntimeException error;

        private QueueItem(Row row, RuntimeException error) {
            this.row = row;
            this.error = error;
        }

        static QueueItem row(Row row) {
            return new QueueItem(row, null);
        }

        static QueueItem error(RuntimeException error) {
            return new QueueItem(null, error);
        }

        boolean isFileDone() {
            return this == FILE_DONE;
        }

        boolean isError() {
            return error != null;
        }
    }
}
