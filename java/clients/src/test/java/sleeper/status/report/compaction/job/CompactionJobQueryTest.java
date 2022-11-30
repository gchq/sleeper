/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.TestCompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.status.report.compaction.job.CompactionJobStatusReporter.QueryType;

public class CompactionJobQueryTest {
    private static final String tableName = "test-table";
    private final CompactionJobStatusStore statusStore = mock(CompactionJobStatusStore.class);
    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
    private final CompactionJobStatus exampleStatus1 = TestCompactionJobStatus.created(
            dataHelper.singleFileCompaction(), Instant.parse("2022-09-22T13:33:12.001Z"));
    private final CompactionJobStatus exampleStatus2 = TestCompactionJobStatus.created(
            dataHelper.singleFileCompaction(), Instant.parse("2022-09-22T13:53:12.001Z"));
    private final List<CompactionJobStatus> exampleStatusList = Arrays.asList(exampleStatus1, exampleStatus2);

    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        QueryType queryType = QueryType.ALL;
        when(statusStore.getAllJobs(tableName)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatuses(queryType);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateUnfinishedQueryWithNoParameters() {
        // Given
        QueryType queryType = QueryType.UNFINISHED;
        when(statusStore.getUnfinishedJobs(tableName)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatuses(queryType);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateDetailedQueryWithSpecifiedJobIds() {
        // Given
        QueryType queryType = QueryType.DETAILED;
        String queryParameters = "job1,job2";
        when(statusStore.getJob("job1")).thenReturn(Optional.of(exampleStatus1));
        when(statusStore.getJob("job2")).thenReturn(Optional.of(exampleStatus2));

        // When
        List<CompactionJobStatus> statuses = queryStatusesWithParams(queryType, queryParameters);

        // Then
        assertThat(statuses).containsExactly(exampleStatus1, exampleStatus2);
    }

    @Test
    public void shouldCreateRangeQueryWithSpecifiedDates() {
        // Given
        QueryType queryType = QueryType.RANGE;
        String queryParameters = "20221123115442,20221130115442";
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableName, start, end)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatusesWithParams(queryType, queryParameters);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultDates() {
        // Given
        QueryType queryType = QueryType.RANGE;
        Instant start = Instant.parse("2022-11-30T07:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableName, start, end)).thenReturn(exampleStatusList);

        // When
        List<CompactionJobStatus> statuses = queryStatusesAtTime(queryType, end);

        // Then
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    private List<CompactionJobStatus> queryStatuses(QueryType queryType) {
        return queryStatusesWithParams(queryType, null);
    }

    private List<CompactionJobStatus> queryStatusesWithParams(QueryType queryType, String queryParameters) {
        return queryStatuses(queryType, queryParameters, Clock.systemUTC());
    }

    private List<CompactionJobStatus> queryStatusesAtTime(QueryType queryType, Instant time) {
        return queryStatuses(queryType, null,
                Clock.fixed(time, ZoneId.of("UTC")));
    }

    private List<CompactionJobStatus> queryStatuses(QueryType queryType, String queryParameters, Clock clock) {
        return CompactionJobQuery.from(tableName, queryType, queryParameters, clock).run(statusStore);
    }
}
