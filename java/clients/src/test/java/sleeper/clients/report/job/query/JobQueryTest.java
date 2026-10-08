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

package sleeper.clients.report.job.query;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JobQueryTest extends JobQueryTestBase {
    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        JobQueryType queryType = JobQueryType.ALL;
        createExampleJobs();

        // When
        List<CompactionJobStatus> statuses = queryStatuses(queryType);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateUnfinishedQueryWithNoParameters() {
        // Given
        JobQueryType queryType = JobQueryType.UNFINISHED;
        createExampleJobs();

        // When
        List<CompactionJobStatus> statuses = queryStatuses(queryType);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateDetailedQueryWithSpecifiedJobIds() {
        // Given
        JobQueryType queryType = JobQueryType.DETAILED;
        String queryParameters = "job1,job2";
        createExampleJobs();

        // When
        List<CompactionJobStatus> statuses = queryStatusesWithParams(queryType, queryParameters);

        // Then
        assertThat(statuses).containsExactly(exampleStatus1, exampleStatus2);
    }

    @Test
    public void shouldReturnNoDetailedQueryWithNoJobIds() {
        // Given
        JobQueryType queryType = JobQueryType.DETAILED;

        // When
        assertThat(queryFrom(queryType)).isNull();
    }

    @Test
    public void shouldCreateRangeQueryWithSpecifiedDates() {
        // Given
        JobQueryType queryType = JobQueryType.RANGE;
        String queryParameters = "20221123115442,20221130115442";
        createExampleJobs();

        // When
        List<CompactionJobStatus> statuses = queryStatusesWithParams(queryType, queryParameters);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultDates() {
        // Given
        JobQueryType queryType = JobQueryType.RANGE;
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        createExampleJobs();

        // When
        List<CompactionJobStatus> statuses = queryStatusesAtTime(queryType, end);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldFailRangeQueryWhenStartIsAfterEnd() {
        // Given
        JobQueryType queryType = JobQueryType.RANGE;
        String queryParameters = "20221130125442,20221130115442";

        // When / Then
        assertThatThrownBy(() -> queryStatusesWithParams(queryType, queryParameters))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
