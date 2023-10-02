/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.clients.status.report.query;

import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.TrackedQuery;

import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

public class StandardQueryTrackerReporter implements QueryTrackerReporter {
    private PrintStream out;
    private final TableField state;
    private final TableField queryId;
    private final TableField subQueryId;
    private final TableField lastUpdateTime;
    private final TableField recordCount;
    private final TableWriterFactory tableFactory;

    public StandardQueryTrackerReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        state = tableFactoryBuilder.addField("STATE");
        queryId = tableFactoryBuilder.addField("QUERY_ID");
        subQueryId = tableFactoryBuilder.addField("SUB_QUERY_ID");
        lastUpdateTime = tableFactoryBuilder.addField("LAST_UPDATE_TIME");
        recordCount = tableFactoryBuilder.addField("RECORD_COUNT");
        tableFactory = tableFactoryBuilder.build();
    }

    @Override
    public void report(TrackerQuery queryType, List<TrackedQuery> trackedQueries) {
        out.println();
        out.println("Query Tracker Report");
        out.println("--------------------");
        if (TrackerQuery.ALL == queryType) {
            printAllSummary(queryType, trackedQueries);
        } else if (TrackerQuery.QUEUED == queryType) {
            printQueuedSummary(trackedQueries.stream());
        }
        tableFactory.tableBuilder().itemsAndWriter(trackedQueries, this::writeQueryFields)
                .build().write(out);
    }

    private void printAllSummary(TrackerQuery queryType, List<TrackedQuery> trackedQueries) {
        out.printf("Total queries: %d%n", trackedQueries.size());
        out.println();
        printQueuedSummary(trackedQueries.stream().filter(query -> query.getLastKnownState() == QueryState.QUEUED));
        out.printf("Total queries in progress: %d%n", countQueriesWithState(trackedQueries, QueryState.IN_PROGRESS));
        out.printf("Total queries completed: %d%n", countQueriesWithState(trackedQueries, QueryState.COMPLETED));
        out.println();
        out.printf("Total queries partially failed: %d%n", countQueriesWithState(trackedQueries, QueryState.PARTIALLY_FAILED));
        out.printf("Total queries failed: %d%n", countQueriesWithState(trackedQueries, QueryState.FAILED));
    }

    private void printQueuedSummary(Stream<TrackedQuery> queuedQueries) {
        out.printf("Total queries queued: %d%n", queuedQueries.count());
    }

    private void writeQueryFields(TrackedQuery trackedQuery, TableRow.Builder builder) {
        builder.value(state, trackedQuery.getLastKnownState())
                .value(queryId, trackedQuery.getQueryId())
                .value(subQueryId, "-".equals(trackedQuery.getSubQueryId()) ? "" : trackedQuery.getSubQueryId())
                .value(lastUpdateTime, Instant.ofEpochMilli(trackedQuery.getLastUpdateTime()))
                .value(recordCount, trackedQuery.getRecordCount());
    }

    private static long countQueriesWithState(List<TrackedQuery> trackedQueries, QueryState queryState) {
        return trackedQueries.stream().filter(query -> query.getLastKnownState() == queryState).count();
    }
}
