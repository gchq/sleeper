/*
 * Copyright 2022-2024 Crown Copyright
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.query.core.tracker.TrackedQuery;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.query.QueryTrackerReporterTestHelper.getStandardReport;
import static sleeper.clients.status.report.query.QueryTrackerReporterTestHelper.mixedQueries;
import static sleeper.clients.status.report.query.QueryTrackerReporterTestHelper.queryWithSubqueries;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryCompleted;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryFailed;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryInProgress;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryPartiallyFailed;
import static sleeper.query.runner.tracker.TrackedQueryTestHelper.queryQueued;

public class StandardQueryTrackerReporterTest {
    @Nested
    @DisplayName("All tracked queries")
    class AllTrackedQueries {
        @Test
        void shouldRunReportWithNoTrackedQueries() throws Exception {
            // When
            List<TrackedQuery> noQueries = List.of();

            // Then
            assertThat(getStandardReport(TrackerQuery.ALL, noQueries))
                    .isEqualTo(example("reports/query/standard/all/noQueries.txt"));
        }

        @Test
        void shouldRunReportWithMixedQueries() throws Exception {
            // When/Then
            assertThat(getStandardReport(TrackerQuery.ALL, mixedQueries()))
                    .isEqualTo(example("reports/query/standard/all/mixedQueries.txt"));
        }

        @Test
        void shouldRunReportWithSubQueries() throws Exception {
            // When/Then
            assertThat(getStandardReport(TrackerQuery.ALL, queryWithSubqueries()))
                    .isEqualTo(example("reports/query/standard/all/queryWithSubqueries.txt"));
        }
    }

    @Nested
    @DisplayName("Tracked queries by state")
    class TrackedQueriesByState {
        @Test
        void shouldRunReportWithQueuedQueries() throws Exception {
            // Given
            List<TrackedQuery> queuedQueries = List.of(
                    queryQueued("test-query-1", Instant.parse("2023-09-28T18:50:00Z")));

            // When/Then
            assertThat(getStandardReport(TrackerQuery.QUEUED, queuedQueries))
                    .isEqualTo(example("reports/query/standard/state/queuedQueries.txt"));
        }

        @Test
        void shouldRunReportWithInProgressQueries() throws Exception {
            // Given
            List<TrackedQuery> inProgressQueries = List.of(
                    queryInProgress("test-query-1", Instant.parse("2023-09-28T18:50:00Z")));

            // When/Then
            assertThat(getStandardReport(TrackerQuery.IN_PROGRESS, inProgressQueries))
                    .isEqualTo(example("reports/query/standard/state/inProgressQueries.txt"));
        }

        @Test
        void shouldRunReportWithCompletedQueries() throws Exception {
            // Given
            List<TrackedQuery> completedQueries = List.of(
                    queryCompleted("test-query-1", Instant.parse("2023-09-28T18:50:00Z"), 456L));

            // When/Then
            assertThat(getStandardReport(TrackerQuery.COMPLETED, completedQueries))
                    .isEqualTo(example("reports/query/standard/state/completedQueries.txt"));
        }

        @Test
        void shouldRunReportWithFailedQueries() throws Exception {
            // Given
            List<TrackedQuery> failedQueries = List.of(
                    queryPartiallyFailed("test-query-1", Instant.parse("2023-09-28T18:50:00Z"), 123L, "Test failure 1"),
                    queryFailed("test-query-2", Instant.parse("2023-09-28T18:52:00Z"), "Test failure 2"));

            // When/Then
            assertThat(getStandardReport(TrackerQuery.FAILED, failedQueries))
                    .isEqualTo(example("reports/query/standard/state/failedQueries.txt"));
        }
    }
}
