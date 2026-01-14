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

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.query.core.tracker.TrackedQuery;

import java.time.Instant;
import java.util.List;

import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryCompleted;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryFailed;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryInProgress;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryPartiallyFailed;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryQueued;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.subQueryInProgress;

/**
 * Helpers for testing reports based on the query tracker. Creates example query tracking data.
 */
public class QueryTrackerReporterTestHelper {
    private QueryTrackerReporterTestHelper() {
    }

    /**
     * Creates a mix of queries in different statuses.
     *
     * @return the query tracker data
     */
    public static List<TrackedQuery> mixedQueries() {
        return List.of(
                queryQueued("test-query-1", Instant.parse("2023-09-28T18:50:00Z")),
                queryInProgress("test-query-2", Instant.parse("2023-09-28T18:52:00Z")),
                queryCompleted("test-query-3", Instant.parse("2023-09-28T18:54:00Z"), 456L),
                queryPartiallyFailed("test-query-4", Instant.parse("2023-09-28T18:56:00Z"), 123L, "Error: Query partially failed"),
                queryFailed("test-query-5", Instant.parse("2023-09-28T18:58:00Z"), "Error: Query failed"));
    }

    /**
     * Creates data for a single query with two sub-queries.
     *
     * @return the query tracker data
     */
    public static List<TrackedQuery> queryWithSubqueries() {
        return List.of(
                queryInProgress("parent-query-1", Instant.parse("2023-09-28T19:15:00Z")),
                subQueryInProgress("parent-query-1", "sub-query-1", Instant.parse("2023-09-28T19:16:00Z")),
                subQueryInProgress("parent-query-1", "sub-query-2", Instant.parse("2023-09-28T19:17:00Z")));
    }

    /**
     * Creates a report in standard, human readable format.
     *
     * @param  query          the reporting query used to retrieve the data from the query tracker
     * @param  trackedQueries the data from the query tracker
     * @return                the report
     */
    public static String getStandardReport(TrackerQuery query, List<TrackedQuery> trackedQueries) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new StandardQueryTrackerReporter(output.getPrintStream())
                .report(query, trackedQueries);
        return output.toString();
    }

    /**
     * Creates a report in JSON format.
     *
     * @param  query          the reporting query used to retrieve the data from the query tracker
     * @param  trackedQueries the data from the query tracker
     * @return                the report
     */
    public static String getJsonReport(TrackerQuery query, List<TrackedQuery> trackedQueries) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new JsonQueryTrackerReporter(output.getPrintStream())
                .report(query, trackedQueries);
        return output.toString();
    }
}
