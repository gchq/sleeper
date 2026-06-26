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

import org.apache.commons.lang3.exception.UncheckedException;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.ConcatenatingIterator;
import sleeper.core.iterator.closeable.PrefetchingCloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.function.Supplier;

/**
 * Runs queries against a Sleeper table by querying the state store and data files directly. An instance of this class
 * cannot be used concurrently in multiple threads, due to how partitions are cached.
 */
public class ParQueryExecutor {

    private final QueryPlanner queryPlanner;
    private final List<LeafPartitionQueryExecutor> leafQueryExecutors;

    public ParQueryExecutor(QueryPlanner queryPlanner, List<LeafPartitionQueryExecutor> leafQueryExecutors) {
        this.queryPlanner = queryPlanner;
        this.leafQueryExecutors = leafQueryExecutors;
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

    public CloseableIterator<Row> execute(Query query) throws QueryException {
        List<LeafPartitionQuery> leafPartitionQueries = queryPlanner.splitIntoLeafPartitionQueries(query);
        int leafPartitionCount = leafPartitionQueries.size();
        LinkedBlockingQueue<LeafPartitionQuery> workQueue = new LinkedBlockingQueue<>(leafPartitionQueries);
        SynchronousQueue<CloseableIterator<Row>> outputQueue = new SynchronousQueue<>(true);
        ExecutorService es = Executors.newFixedThreadPool(leafQueryExecutors.size());

        for (LeafPartitionQueryExecutor le : leafQueryExecutors) {
            es.submit(() -> {
                try {
                    LeafPartitionQuery leafQuery;
                    while ((leafQuery = workQueue.poll()) != null) {
                        System.out.printf("Thread %s pulled work item %s%n", Thread.currentThread().getName(), leafQuery.getLeafPartitionId());
                        try {
                            CloseableIterator<Row> rowFeed = le.getRows(leafQuery);
                            System.out.printf("Thread %s offering row iterator%n", Thread.currentThread().getName());
                            PrefetchingCloseableIterator<Row> prefetched = new PrefetchingCloseableIterator<>(rowFeed, 10);
                            outputQueue.put(prefetched);
                        } catch (QueryException e) {
                            // could use a Result type here
                            throw new RuntimeException("Exception returning rows for leaf partition " + leafQuery, e);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new UncheckedException(e);
                }
            });
        }
        System.out.println("All threads started...");
        List<Supplier<CloseableIterator<Row>>> iterators = new ArrayList<>();
        for (int i = 0; i < leafPartitionCount; i++) {
            iterators.add(() -> {
                try {
                    System.out.printf("Thread %s receiving a row iterator%n", Thread.currentThread().getName());
                    CloseableIterator<Row> rowIt = outputQueue.take();
                    System.out.printf("Thread %s returning to client%n", Thread.currentThread().getName());
                    return rowIt;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new UncheckedException(e);
                }
            });
        }

        return new ConcatenatingIterator(iterators);
    }
}
