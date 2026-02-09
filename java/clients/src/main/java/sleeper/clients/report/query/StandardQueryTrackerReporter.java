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

package sleeper.clients.report.query;

import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Creates reports in human-readable string format on the status of queries. This produces a table.
 */
public class StandardQueryTrackerReporter implements QueryTrackerReporter {
    private PrintStream out;
    private final TableField state;
    private final TableField queryId;
    private final TableField subQueryId;
    private final TableField lastUpdateTime;
    private final TableField rowCount;
    private final TableField errorMessage;
    private final TableWriterFactory tableFactory;

    public StandardQueryTrackerReporter() {
        this(System.out);
    }

    public StandardQueryTrackerReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        state = tableFactoryBuilder.addField("STATE");
        queryId = tableFactoryBuilder.addField("QUERY_ID");
        subQueryId = tableFactoryBuilder.addField("SUB_QUERY_ID");
        lastUpdateTime = tableFactoryBuilder.addField("LAST_UPDATE_TIME");
        rowCount = tableFactoryBuilder.addField("ROW_COUNT");
        errorMessage = tableFactoryBuilder.addField("ERRORS");
        tableFactory = tableFactoryBuilder.build();
    }

    @Override
    public void report(TrackerQuery queryType, List<TrackedQuery> trackedQueries) {
        out.println();
        out.println("Query Tracker Report");
        out.println("--------------------");
        if (TrackerQuery.ALL == queryType) {
            printAllSummary(trackedQueries);
        } else if (TrackerQuery.QUEUED == queryType) {
            printQueuedSummary(trackedQueries.size());
        } else if (TrackerQuery.IN_PROGRESS == queryType) {
            printInProgressSummary(trackedQueries.size());
        } else if (TrackerQuery.COMPLETED == queryType) {
            printCompletedSummary(trackedQueries.size());
        } else if (TrackerQuery.FAILED == queryType) {
            printFailedSummary(trackedQueries);
        }
        tableFactory.tableBuilder().itemsAndWriter(trackedQueries, this::writeQueryFields)
                .showField(showErrorsField(queryType, trackedQueries), errorMessage)
                .build().write(out);
    }

    private static boolean showErrorsField(TrackerQuery queryType, List<TrackedQuery> trackedQueries) {
        if (TrackerQuery.FAILED == queryType) {
            return true;
        } else {
            return TrackerQuery.ALL == queryType &&
                    trackedQueries.stream().anyMatch(query -> Objects.nonNull(query.getErrorMessage()));
        }
    }

    private void printAllSummary(List<TrackedQuery> trackedQueries) {
        out.printf("Total queries: %d%n", trackedQueries.size());
        out.println();
        printQueuedSummary(countQueriesWithState(trackedQueries, QueryState.QUEUED));
        printInProgressSummary(countQueriesWithState(trackedQueries, QueryState.IN_PROGRESS));
        printCompletedSummary(countQueriesWithState(trackedQueries, QueryState.COMPLETED));
        out.println();
        printFailedSummary(trackedQueries);
    }

    private void printQueuedSummary(long queryCount) {
        out.printf("Total queries queued: %d%n", queryCount);
    }

    private void printInProgressSummary(long queryCount) {
        out.printf("Total queries in progress: %d%n", queryCount);
    }

    private void printCompletedSummary(long queryCount) {
        out.printf("Total queries completed: %d%n", queryCount);
    }

    private void printFailedSummary(List<TrackedQuery> failedQueries) {
        out.printf("Total queries partially failed: %d%n", countQueriesWithState(failedQueries, QueryState.PARTIALLY_FAILED));
        out.printf("Total queries failed: %d%n", countQueriesWithState(failedQueries, QueryState.FAILED));
    }

    private void writeQueryFields(TrackedQuery trackedQuery, TableRow.Builder builder) {
        builder.value(state, trackedQuery.getLastKnownState())
                .value(queryId, trackedQuery.getQueryId())
                .value(subQueryId, trackedQuery.getSubQueryId())
                .value(lastUpdateTime, Instant.ofEpochMilli(trackedQuery.getLastUpdateTime()))
                .value(rowCount, trackedQuery.getRowCount())
                .value(errorMessage, trackedQuery.getErrorMessage());
    }

    private static long countQueriesWithState(List<TrackedQuery> trackedQueries, QueryState queryState) {
        return trackedQueries.stream().filter(query -> query.getLastKnownState() == queryState).count();
    }
}
