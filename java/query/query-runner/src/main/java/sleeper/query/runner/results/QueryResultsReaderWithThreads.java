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
package sleeper.query.runner.results;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.util.PollWithRetries;
import sleeper.query.core.tracker.TrackedQuery;

import java.util.List;
import java.util.function.Supplier;

/**
 * Reads the results of a query with a set number of threads to read results files. With more than one thread, the
 * results files are read in parallel and the rows are interleaved across files in no particular order.
 */
class QueryResultsReaderWithThreads implements QueryResultsReader {
    private final QueryResultsReader reader;
    private final int threads;

    QueryResultsReaderWithThreads(QueryResultsReader reader, int threads) {
        this.reader = reader;
        this.threads = threads;
    }

    @Override
    public List<ResultsFile> listResultsFiles(String tableName, String queryId) {
        return reader.listResultsFiles(tableName, queryId);
    }

    @Override
    public CloseableIterator<Row> getResults(String tableName, String queryId) {
        if (threads == 1) {
            return reader.getResults(tableName, queryId);
        }
        List<ResultsFile> files = listResultsFiles(tableName, queryId);
        if (files.isEmpty()) {
            throw new QueryResultsNotFoundException(queryId, null);
        }
        return new ParallelResultsFileIterator(files, threads);
    }

    @Override
    public CloseableIterator<Row> streamResults(
            String tableName, String queryId, Supplier<TrackedQuery> statusSupplier, PollWithRetries poll) {
        return new StreamingQueryResultsIterator(tableName, queryId, reader, statusSupplier, poll, threads);
    }
}
