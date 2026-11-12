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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.api.QueryResultsReader.ResultsFile;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.util.PollWithRetries;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Streams through the results of a query while it is still running. Results files published by subqueries that have
 * already finished are read as they appear, found by polling the query tracker and the results output. Note that each
 * wait for more results has the full polling timeout, so the timeout applies to the time with no new results, rather
 * than the time for the whole query.
 * <p>
 * If the query fails or partially fails, this throws a {@link QueryFailedException} from {@link #hasNext}. Rows from
 * subqueries that finished before the failure may already have been returned, so any rows read so far must be treated
 * as a partial result.
 */
public class StreamingQueryResultsIterator implements CloseableIterator<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingQueryResultsIterator.class);

    private final String tableName;
    private final String queryId;
    private final QueryResultsReader resultsReader;
    private final Supplier<TrackedQuery> statusSupplier;
    private final PollWithRetries poll;
    private final Set<String> seenFiles = new HashSet<>();
    private final Deque<ResultsFile> unreadFiles = new ArrayDeque<>();
    private CloseableIterator<Row> currentFile;
    private Long finishedSubQueryCountAtLastListing;
    private boolean queryFinished;

    /**
     * Creates an iterator streaming through the results of a query that has been submitted.
     *
     * @param tableName      the name of the table the query ran against
     * @param queryId        the query ID
     * @param resultsReader  the reader to find results files published for the query
     * @param statusSupplier retrieves the entry for the query in the query tracker, null if it is not tracked yet
     * @param poll           the polling interval and timeout for each wait for more results
     */
    public StreamingQueryResultsIterator(
            String tableName, String queryId, QueryResultsReader resultsReader,
            Supplier<TrackedQuery> statusSupplier, PollWithRetries poll) {
        this.tableName = tableName;
        this.queryId = queryId;
        this.resultsReader = resultsReader;
        this.statusSupplier = statusSupplier;
        this.poll = poll;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (currentFile != null) {
                if (currentFile.hasNext()) {
                    return true;
                }
                closeCurrentFile();
            }
            if (!unreadFiles.isEmpty()) {
                ResultsFile file = unreadFiles.poll();
                LOGGER.info("Reading results file {} of query {}", file.name(), queryId);
                currentFile = file.contents().get();
                continue;
            }
            if (queryFinished) {
                return false;
            }
            waitForNewResults();
        }
    }

    @Override
    @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR") // hasNext sets currentFile when it returns true
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentFile.next();
    }

    @Override
    public void close() throws IOException {
        if (currentFile != null) {
            CloseableIterator<Row> file = currentFile;
            currentFile = null;
            file.close();
        }
    }

    /**
     * Polls the query tracker until new results files have been published or the query has finished. When the query
     * has finished, a final listing finds any remaining files, as every subquery publishes its results before it is
     * reported as finished.
     */
    private void waitForNewResults() {
        try {
            poll.pollUntil("more results of query " + queryId + " are published", () -> {
                TrackedQuery status = statusSupplier.get();
                if (status == null) {
                    LOGGER.info("Query {} is not yet in the query tracker, waiting for it to start", queryId);
                    return false;
                }
                QueryState state = status.getLastKnownState();
                if (state == QueryState.FAILED || state == QueryState.PARTIALLY_FAILED) {
                    throw new QueryFailedException(status);
                }
                if (state.isFinished()) {
                    LOGGER.info("Query {} finished with state {}, rows returned: {}",
                            queryId, state, status.getRowCount());
                    queryFinished = true;
                    if (status.getRowCount() != null && status.getRowCount() == 0) {
                        // An optimisation to avoid reading empty files when no results have been returned
                        unreadFiles.clear();
                    } else {
                        listNewFiles();
                    }
                    return true;
                }
                if (newSubQueriesFinishedSinceLastListing(status)) {
                    listNewFiles();
                }
                return !unreadFiles.isEmpty();
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for results of query " + queryId, e);
        }
    }

    /**
     * Checks whether listing the results output could find new files. The query tracker counts finished subqueries,
     * and each subquery publishes its results before it is counted, so there is nothing new to list until the count
     * changes. The count is unknown before the query is split into subqueries, so we list every poll until it is set.
     *
     * @param  status the entry for the query in the query tracker
     * @return        true if new subqueries finished since the last listing, or the count is unknown
     */
    private boolean newSubQueriesFinishedSinceLastListing(TrackedQuery status) {
        Long finishedCount = status.getFinishedSubQueryCount();
        if (finishedCount == null) {
            return true;
        }
        if (finishedCount.equals(finishedSubQueryCountAtLastListing)) {
            return false;
        }
        finishedSubQueryCountAtLastListing = finishedCount;
        return true;
    }

    private void listNewFiles() {
        for (ResultsFile file : resultsReader.listResultsFiles(tableName, queryId)) {
            if (seenFiles.add(file.name())) {
                unreadFiles.add(file);
            }
        }
    }

    private void closeCurrentFile() {
        try {
            close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close results file of query " + queryId, e);
        }
    }
}
