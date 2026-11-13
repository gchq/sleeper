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

import sleeper.clients.api.QueryResultsReader.ResultsFile;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterates over the rows in a set of query results files, reading the files in parallel in a thread pool, with the
 * output exposed as an iterator. So results are consumed sequentially but files are read in parallel. The rows are
 * returned in no particular order, as results may be interleaved from different results files.
 * <p>
 * This class is not thread-safe; do not use without external synchronisation.
 */
class ParallelResultsFileIterator implements CloseableIterator<Row> {

    private final ParallelResultsFileReader reader;
    private Row nextRow = null;

    /**
     * Creates an iterator from a ParallelResultsFileReader.
     *
     * @param files   the results files to read
     * @param threads the number of threads to read files in parallel
     */
    ParallelResultsFileIterator(List<ResultsFile> files, int threads) {
        this.reader = new ParallelResultsFileReader(threads);
        files.forEach(reader::submit);
    }

    @Override
    public boolean hasNext() {
        if (nextRow == null) {
            nextRow = reader.takeNextRow();
        }
        return nextRow != null;
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }

    @Override
    public void close() {
        reader.close();
    }
}
