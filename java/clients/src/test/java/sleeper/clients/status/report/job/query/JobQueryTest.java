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

package sleeper.clients.status.report.job.query;

import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.job.query.JobQuery.Type;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class JobQueryTest extends JobQueryTestBase {
    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        Type queryType = Type.ALL;
        when(statusStore.getAllJobs(tableId)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatuses(queryType);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateUnfinishedQueryWithNoParameters() {
        // Given
        Type queryType = Type.UNFINISHED;
        when(statusStore.getUnfinishedJobs(tableId)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatuses(queryType);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateDetailedQueryWithSpecifiedJobIds() {
        // Given
        Type queryType = Type.DETAILED;
        String queryParameters = "job1,job2";
        when(statusStore.getJob("job1")).thenReturn(Optional.of(exampleStatus1));
        when(statusStore.getJob("job2")).thenReturn(Optional.of(exampleStatus2));

        // When
        List<CompactionJobStatus> statuses = queryStatusesWithParams(queryType, queryParameters);

        // Then
        assertThat(statuses).containsExactly(exampleStatus1, exampleStatus2);
    }

    @Test
    public void shouldFailDetailedQueryWithNoJobIds() {
        // Given
        Type queryType = Type.DETAILED;

        // When
        assertThatThrownBy(() -> queryStatuses(queryType))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldCreateRangeQueryWithSpecifiedDates() {
        // Given
        Type queryType = Type.RANGE;
        String queryParameters = "20221123115442,20221130115442";
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatusesWithParams(queryType, queryParameters);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultDates() {
        // Given
        Type queryType = Type.RANGE;
        Instant start = Instant.parse("2022-11-30T07:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatusesAtTime(queryType, end);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldFailRangeQueryWhenStartIsAfterEnd() {
        // Given
        Type queryType = Type.RANGE;
        String queryParameters = "20221130125442,20221130115442";

        // When / Then
        assertThatThrownBy(() -> queryStatusesWithParams(queryType, queryParameters))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
