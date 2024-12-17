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
import sleeper.compaction.core.job.query.CompactionJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class JobQueryPromptTest extends JobQueryTestBase {

    private static final String QUERY_TYPE_PROMPT = "All (a), Detailed (d), range (r), or unfinished (u) query? \n";
    private static final String DETAILED_JOB_ID_PROMPT = "Enter jobId to get detailed information about: \n";
    private static final String RANGE_START_PROMPT = "Enter range start in format yyyyMMddHHmmss (default is 4 hours ago): \n";
    private static final String RANGE_END_PROMPT = "Enter range end in format yyyyMMddHHmmss (default is now): \n";

    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        when(statusStore.getAllJobs(tableId)).thenReturn(exampleStatusList);
        in.enterNextPrompt("a");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateUnfinishedQueryWithNoParameters() {
        // Given
        when(statusStore.getUnfinishedJobs(tableId)).thenReturn(exampleStatusList);
        in.enterNextPrompt("u");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateDetailedQueryWithSpecifiedJobIds() {
        // Given
        String queryParameters = "job1,job2";
        when(statusStore.getJob("job1")).thenReturn(Optional.of(exampleStatus1));
        when(statusStore.getJob("job2")).thenReturn(Optional.of(exampleStatus2));
        in.enterNextPrompts("d", queryParameters);

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + DETAILED_JOB_ID_PROMPT);
        assertThat(statuses).containsExactly(exampleStatus1, exampleStatus2);
    }

    @Test
    public void shouldCreateDetailedQueryWithNoJobIds() {
        // Given
        String queryParameters = "";
        in.enterNextPrompts("d", queryParameters);

        // When
        JobQuery query = queryByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + DETAILED_JOB_ID_PROMPT);
        assertThat(query).isNull();
    }

    @Test
    public void shouldCreateRangeQueryWithSpecifiedDates() {
        // Given
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);
        in.enterNextPrompts("r", "20221123115442", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultEndTime() {
        // Given
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);
        in.enterNextPrompts("r", "20221123115442", "");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultStartTime() {
        // Given
        Instant start = Instant.parse("2022-11-30T07:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);
        in.enterNextPrompts("r", "", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldRepeatRangeQueryPromptWithInvalidStartTime() {
        // Given
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);
        in.enterNextPrompts("r", "abc", "20221123115442", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldRepeatRangeQueryPromptWithInvalidEndTime() {
        // Given
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        when(statusStore.getJobsInTimePeriod(tableId, start, end)).thenReturn(exampleStatusList);
        in.enterNextPrompts("r", "20221123115442", "abc", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldRepeatQueryTypePromptWithInvalidQueryType() {
        // Given
        when(statusStore.getAllJobs(tableId)).thenReturn(exampleStatusList);
        in.enterNextPrompts("abc", "a");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + QUERY_TYPE_PROMPT);
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldReturnNoQueryWhenNoQueryTypeEntered() {
        // Given
        in.enterNextPrompts("");

        // When
        JobQuery query = queryByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT);
        assertThat(query).isNull();
    }

    private List<CompactionJobStatus> queryStatusByPrompt() {
        return queryStatuses(Type.PROMPT);
    }

    private List<CompactionJobStatus> queryStatusByPromptAtTime(Instant time) {
        return queryStatusesAtTime(Type.PROMPT, time);
    }

    private JobQuery queryByPrompt() {
        return queryFrom(Type.PROMPT);
    }
}
