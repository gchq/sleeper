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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.ConcatenatingIterator;
import sleeper.core.row.Row;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Runs queries against a Sleeper table by querying the state store and data files directly. An instance of this class
 * cannot be used concurrently in multiple threads, due to how partitions are cached.
 */
public class QueryExecutor {
    private static final int DEFAULT_BUFFER_SIZE = 10000;

    private final QueryPlanner queryPlanner;
    private final LeafPartitionQueryExecutor leafQueryExecutor;
    private final ExecutorService executorService;
    private final int bufferSize;

    public QueryExecutor(QueryPlanner queryPlanner, LeafPartitionQueryExecutor leafQueryExecutor) {
        this(queryPlanner, leafQueryExecutor, null);
    }

    /**
     * Creates an executor that runs leaf partition sub-queries in parallel using the provided thread pool. Background
     * threads read rows from each partition and feed them into a shared queue; the returned iterator drains that queue.
     * Parallelism is bounded by the thread pool size; excess sub-queries queue until a thread is free.
     *
     * @param queryPlanner      the planner that splits a query into leaf partition sub-queries
     * @param leafQueryExecutor the executor that retrieves rows for a single leaf partition
     * @param executorService   the thread pool used to run sub-queries concurrently, or null for sequential execution
     */
    public QueryExecutor(QueryPlanner queryPlanner, LeafPartitionQueryExecutor leafQueryExecutor, ExecutorService executorService) {
        this(queryPlanner, leafQueryExecutor, executorService, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates an executor that runs leaf partition sub-queries in parallel using the provided thread pool, with a
     * configurable row buffer size. The buffer limits how far ahead producers can run relative to the consumer.
     *
     * @param queryPlanner      the planner that splits a query into leaf partition sub-queries
     * @param leafQueryExecutor the executor that retrieves rows for a single leaf partition
     * @param executorService   the thread pool used to run sub-queries concurrently, or null for sequential execution
     * @param bufferSize        the maximum number of rows held in the shared queue at any one time
     */
    public QueryExecutor(QueryPlanner queryPlanner, LeafPartitionQueryExecutor leafQueryExecutor, ExecutorService executorService, int bufferSize) {
        this.queryPlanner = queryPlanner;
        this.leafQueryExecutor = leafQueryExecutor;
        this.executorService = executorService;
        this.bufferSize = bufferSize;
    }

    /**
     * Initialises the query splitter if the next initialise time has passed.
     *
     * @param  now                 the time now
     * @throws StateStoreException if the state store can't be accessed
     */
    public void initIfNeeded(Instant now) throws StateStoreException {
        queryPlanner.initIfNeeded(now);
    }

    /**
     * Executes a query. This method first splits up the query into one or more
     * {@link LeafPartitionQuery}s. For each of these a Supplier of CloseableIterator
     * is created. This is done using suppliers to avoid the initialisation of
     * row retrievers until they are needed. In the case of Parquet files,
     * initialisation of the readers requires reading the footers of the file
     * which takes a little time. If a query spanned many leaf partitions and
     * each leaf partition had many file references, then the initialisation time
     * could be high. Using suppliers ensures that only files for a single
     * leaf partition are opened at a time.
     *
     * @param  query          the query
     * @return                an iterator containing the relevant rows
     * @throws QueryException if it errors
     */
    public CloseableIterator<Row> execute(Query query) throws QueryException {
        List<LeafPartitionQuery> leafPartitionQueries = queryPlanner.splitIntoLeafPartitionQueries(query);
        if (executorService != null) {
            return executeInParallel(leafPartitionQueries);
        } else {
            return executeSerially(leafPartitionQueries);
        }
    }

    private CloseableIterator<Row> executeSerially(List<LeafPartitionQuery> leafPartitionQueries) {
        List<Supplier<CloseableIterator<Row>>> iteratorSuppliers = createRowIteratorSuppliers(leafPartitionQueries);
        return new ConcatenatingIterator(iteratorSuppliers);
    }

    private CloseableIterator<Row> executeInParallel(List<LeafPartitionQuery> leafPartitionQueries) {
        BlockingQueue<ParallelQueryIterator.QueueItem> queue = new ArrayBlockingQueue<>(bufferSize);
        AtomicBoolean closed = new AtomicBoolean(false);
        List<Future<?>> futures = leafPartitionQueries.stream()
                .map(leafPartitionQuery -> (Future<?>) executorService.submit(() -> {
                    boolean sentTerminal = false;
                    try (CloseableIterator<Row> rows = leafQueryExecutor.getRows(leafPartitionQuery)) {
                        while (rows.hasNext() && !closed.get()) {
                            queue.put(ParallelQueryIterator.QueueItem.row(rows.next()));
                        }
                    } catch (QueryException | RuntimeException e) {
                        putIfOpen(queue, closed, ParallelQueryIterator.QueueItem.error(
                                e instanceof RuntimeException re ? re
                                        : new RuntimeException("Exception returning rows for leaf partition " + leafPartitionQuery, e)));
                        sentTerminal = true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        sentTerminal = true; // close() was called, DONE not needed
                    } catch (IOException ignored) {
                        // thrown by close() - nothing meaningful to do with a close failure here
                    } finally {
                        if (!sentTerminal) {
                            putIfOpen(queue, closed, ParallelQueryIterator.QueueItem.DONE);
                        }
                    }
                }))
                .collect(Collectors.toList());
        return new ParallelQueryIterator(queue, futures, closed);
    }

    private static void putIfOpen(BlockingQueue<ParallelQueryIterator.QueueItem> queue,
            AtomicBoolean closed, ParallelQueryIterator.QueueItem item) {
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

    private List<Supplier<CloseableIterator<Row>>> createRowIteratorSuppliers(List<LeafPartitionQuery> leafPartitionQueries) {
        List<Supplier<CloseableIterator<Row>>> iterators = new ArrayList<>();

        for (LeafPartitionQuery leafPartitionQuery : leafPartitionQueries) {
            iterators.add(() -> {
                try {
                    return leafQueryExecutor.getRows(leafPartitionQuery);
                } catch (QueryException e) {
                    throw new RuntimeException("Exception returning rows for leaf partition " + leafPartitionQuery, e);
                }
            });
        }
        return iterators;
    }
}
