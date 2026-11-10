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

package sleeper.clients.report.query;

import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Creates reports in human-readable string format on the status of queries. This produces a table of queries and a
 * table of subqueries, as the two have different fields (a query processed by lambda has counters of its finished
 * subqueries; a subquery has an ID of the attempt at processing the query that last updated it).
 */
public class StandardQueryTrackerReporter implements QueryTrackerReporter {
    private final PrintStream out;
    private final TableField queryState;
    private final TableField queryId;
    private final TableField queryLastUpdateTime;
    private final TableField subQueriesFinished;
    private final TableField subQueriesFailed;
    private final TableField queryRowCount;
    private final TableField queryErrorMessage;
    private final TableWriterFactory queriesTableFactory;
    private final TableField subQueryState;
    private final TableField subQueryQueryId;
    private final TableField subQueryId;
    private final TableField subQueryAttemptId;
    private final TableField subQueryLastUpdateTime;
    private final TableField subQueryRowCount;
    private final TableField subQueryErrorMessage;
    private final TableWriterFactory subQueriesTableFactory;

    public StandardQueryTrackerReporter() {
        this(System.out);
    }

    public StandardQueryTrackerReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder queriesBuilder = TableWriterFactory.builder();
        queryState = queriesBuilder.addField("STATE");
        queryId = queriesBuilder.addField("QUERY_ID");
        queryLastUpdateTime = queriesBuilder.addField("LAST_UPDATE_TIME");
        subQueriesFinished = queriesBuilder.addField("SUBQUERIES_FINISHED");
        subQueriesFailed = queriesBuilder.addField("SUBQUERIES_FAILED");
        queryRowCount = queriesBuilder.addField("ROW_COUNT");
        queryErrorMessage = queriesBuilder.addField("ERRORS");
        queriesTableFactory = queriesBuilder.build();
        TableWriterFactory.Builder subQueriesBuilder = TableWriterFactory.builder();
        subQueryState = subQueriesBuilder.addField("STATE");
        subQueryQueryId = subQueriesBuilder.addField("QUERY_ID");
        subQueryId = subQueriesBuilder.addField("SUB_QUERY_ID");
        subQueryAttemptId = subQueriesBuilder.addField("ATTEMPT_ID");
        subQueryLastUpdateTime = subQueriesBuilder.addField("LAST_UPDATE_TIME");
        subQueryRowCount = subQueriesBuilder.addField("ROW_COUNT");
        subQueryErrorMessage = subQueriesBuilder.addField("ERRORS");
        subQueriesTableFactory = subQueriesBuilder.build();
    }

    @Override
    public void report(QueryTrackerQuery queryType, List<TrackedQuery> trackedQueries) {
        List<TrackedQuery> queries = trackedQueries.stream()
                .filter(StandardQueryTrackerReporter::isParentQuery)
                .sorted(Comparator.comparing(TrackedQuery::getQueryId))
                .toList();
        List<TrackedQuery> subQueries = trackedQueries.stream()
                .filter(query -> !isParentQuery(query))
                .sorted(Comparator.comparing(TrackedQuery::getQueryId)
                        .thenComparing(TrackedQuery::getSubQueryId))
                .toList();
        out.println();
        out.println("Query Tracker Report");
        out.println("--------------------");
        printSummary(queryType, queries, subQueries);
        out.println();
        out.println("Queries:");
        queriesTableFactory.tableBuilder().itemsAndWriter(queries, this::writeQueryFields)
                .showField(anyHasError(queries), queryErrorMessage)
                .build().write(out);
        out.println();
        out.println("Subqueries:");
        subQueriesTableFactory.tableBuilder().itemsAndWriter(subQueries, this::writeSubQueryFields)
                .showField(anyHasError(subQueries), subQueryErrorMessage)
                .build().write(out);
    }

    private static boolean isParentQuery(TrackedQuery query) {
        return "-".equals(query.getSubQueryId());
    }

    private static boolean anyHasError(List<TrackedQuery> trackedQueries) {
        return trackedQueries.stream().anyMatch(query -> Objects.nonNull(query.getErrorMessage()));
    }

    private void printSummary(QueryTrackerQuery queryType, List<TrackedQuery> queries, List<TrackedQuery> subQueries) {
        if (QueryTrackerQuery.FOR_QUERY == queryType) {
            printForQuerySummary(queries);
            return;
        }
        if (QueryTrackerQuery.ALL == queryType) {
            printStateSummary(queries, subQueries, QueryState.QUEUED, "queued");
            printStateSummary(queries, subQueries, QueryState.IN_PROGRESS, "in progress");
            printStateSummary(queries, subQueries, QueryState.COMPLETED, "completed");
            printFailedSummary(queries, subQueries);
        } else if (QueryTrackerQuery.QUEUED == queryType) {
            printStateSummary(queries, subQueries, QueryState.QUEUED, "queued");
        } else if (QueryTrackerQuery.IN_PROGRESS == queryType) {
            printStateSummary(queries, subQueries, QueryState.IN_PROGRESS, "in progress");
        } else if (QueryTrackerQuery.COMPLETED == queryType) {
            printStateSummary(queries, subQueries, QueryState.COMPLETED, "completed");
        } else if (QueryTrackerQuery.FAILED == queryType) {
            printFailedSummary(queries, subQueries);
        }
    }

    private void printForQuerySummary(List<TrackedQuery> queries) {
        if (queries.isEmpty()) {
            out.println("Query not found");
            return;
        }
        TrackedQuery query = queries.get(0);
        out.printf("Query %s is %s%n", query.getQueryId(), query.getLastKnownState());
    }

    private void printStateSummary(List<TrackedQuery> queries, List<TrackedQuery> subQueries, QueryState state, String description) {
        out.printf("Queries %s: %d, subqueries %s: %d%n",
                description, countWithState(queries, state),
                description, countWithState(subQueries, state));
    }

    private void printFailedSummary(List<TrackedQuery> queries, List<TrackedQuery> subQueries) {
        out.printf("Queries partially failed: %d, subqueries partially failed: %d%n",
                countWithState(queries, QueryState.PARTIALLY_FAILED),
                countWithState(subQueries, QueryState.PARTIALLY_FAILED));
        out.printf("Queries failed: %d, subqueries failed: %d%n",
                countWithState(queries, QueryState.FAILED),
                countWithState(subQueries, QueryState.FAILED));
    }

    private void writeQueryFields(TrackedQuery trackedQuery, TableRow.Builder builder) {
        builder.value(queryState, trackedQuery.getLastKnownState())
                .value(queryId, trackedQuery.getQueryId())
                .value(queryLastUpdateTime, Instant.ofEpochMilli(trackedQuery.getLastUpdateTime()))
                .value(subQueriesFinished, describeFinishedSubQueries(trackedQuery))
                .value(subQueriesFailed, trackedQuery.getFailedSubQueryCount())
                .value(queryRowCount, describeRowCount(trackedQuery))
                .value(queryErrorMessage, trackedQuery.getErrorMessage());
    }

    private static String describeFinishedSubQueries(TrackedQuery trackedQuery) {
        if (trackedQuery.getExpectedSubQueryCount() == null) {
            return null;
        }
        return trackedQuery.getFinishedSubQueryCount() + "/" + trackedQuery.getExpectedSubQueryCount();
    }

    private static Object describeRowCount(TrackedQuery trackedQuery) {
        if (trackedQuery.getLastKnownState() == QueryState.IN_PROGRESS
                && trackedQuery.getFinishedSubQueryRowCount() != null) {
            return trackedQuery.getFinishedSubQueryRowCount() + " so far";
        }
        return trackedQuery.getRowCount();
    }

    private void writeSubQueryFields(TrackedQuery trackedQuery, TableRow.Builder builder) {
        builder.value(subQueryState, trackedQuery.getLastKnownState())
                .value(subQueryQueryId, trackedQuery.getQueryId())
                .value(subQueryId, trackedQuery.getSubQueryId())
                .value(subQueryAttemptId, trackedQuery.getAttemptId())
                .value(subQueryLastUpdateTime, Instant.ofEpochMilli(trackedQuery.getLastUpdateTime()))
                .value(subQueryRowCount, trackedQuery.getRowCount())
                .value(subQueryErrorMessage, trackedQuery.getErrorMessage());
    }

    private static long countWithState(List<TrackedQuery> trackedQueries, QueryState queryState) {
        return trackedQueries.stream().filter(query -> query.getLastKnownState() == queryState).count();
    }
}
