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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.query.tracker.TrackedQuery;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.query.QueryTrackerReporterTestHelper.getStandardReport;
import static sleeper.clients.status.report.query.QueryTrackerReporterTestHelper.mixedQueries;
import static sleeper.clients.testutil.ClientTestUtils.example;

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
                    .isEqualTo(example("reports/query/noQueries.txt"));
        }

        @Test
        void shouldRunReportWithMixedQueries() throws Exception {
            // When/Then
            assertThat(getStandardReport(TrackerQuery.ALL, mixedQueries()))
                    .isEqualTo(example("reports/query/mixedQueries.txt"));
        }
    }
}
