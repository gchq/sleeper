/*
 * Copyright 2022-2025 Crown Copyright
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
import java.util.function.Supplier;

/**
 * Runs queries against a Sleeper table by querying the state store and data files directly. An instance of this class
 * cannot be used concurrently in multiple threads, due to how partitions are cached.
 */
public class QueryExecutor implements AutoCloseable {

    private final QueryPlanner queryPlanner;
    private final LeafPartitionQueryExecutor leafQueryExecutor;

    public QueryExecutor(QueryPlanner queryPlanner, LeafPartitionQueryExecutor leafQueryExecutor) {
        this.queryPlanner = queryPlanner;
        this.leafQueryExecutor = leafQueryExecutor;
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
        List<Supplier<CloseableIterator<Row>>> iteratorSuppliers = createRowIteratorSuppliers(leafPartitionQueries);
        return new ConcatenatingIterator(iteratorSuppliers);
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

    @Override
    public void close() throws IOException {
        try (leafQueryExecutor) {
        }
    }

}
