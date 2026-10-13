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

public class JobQueryPromptTest extends JobQueryTestBase {

    private static final String QUERY_TYPE_PROMPT = "All (a), Detailed (d), range (r), or unfinished (u) query? \n";
    private static final String DETAILED_JOB_ID_PROMPT = "Enter job IDs to get detailed information about, separated by commas: \n";
    private static final String RANGE_START_PROMPT = "Enter range start in format yyyyMMddHHmmss (default is 4 hours ago): \n";
    private static final String RANGE_END_PROMPT = "Enter range end in format yyyyMMddHHmmss (default is now): \n";

    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        CompactionJobStatus finished = createFinishedJob("finished-job", tableId,
                Instant.parse("2022-11-30T09:00:00Z"), Instant.parse("2022-11-30T10:00:00Z"));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T09:30:00Z"));
        List<CompactionJobStatus> expected = List.of(finished, status2, status1);
        in.enterNextPrompt("a");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldCreateUnfinishedQueryWithNoParameters() {
        // Given
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        createFinishedJob("finished-job", tableId,
                Instant.parse("2022-11-30T09:00:00Z"), Instant.parse("2022-11-30T10:00:00Z"));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T09:30:00Z"));
        in.enterNextPrompt("u");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT);
        assertThat(statuses).containsExactly(status2, status1);
    }

    @Test
    public void shouldCreateDetailedQueryWithSpecifiedJobIds() {
        // Given
        String queryParameters = "job1,job2";
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        createJob("unrequested-job", tableId, Instant.parse("2022-11-30T09:00:00Z"));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T09:30:00Z"));
        in.enterNextPrompts("d", queryParameters);

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + DETAILED_JOB_ID_PROMPT);
        assertThat(statuses).containsExactly(status1, status2);
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
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        CompactionJobStatus insideStart = createFinishedJob("inside-start", tableId,
                start.plusSeconds(1), start.plusSeconds(2));
        createFinishedJob("before-range", tableId, start.minusSeconds(2), start.minusSeconds(1));
        createJob("after-range", tableId, Instant.parse("2022-11-30T11:54:42.000Z").plusSeconds(1));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T08:33:12.001Z"));
        List<CompactionJobStatus> expected = List.of(status2, status1, insideStart);
        in.enterNextPrompts("r", "20221123115442", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultEndTime() {
        // Given
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        CompactionJobStatus insideStart = createFinishedJob("inside-start", tableId,
                start.plusSeconds(1), start.plusSeconds(2));
        createFinishedJob("before-range", tableId, start.minusSeconds(2), start.minusSeconds(1));
        createJob("after-range", tableId, Instant.parse("2022-11-30T11:54:42.000Z").plusSeconds(1));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T08:33:12.001Z"));
        List<CompactionJobStatus> expected = List.of(status2, status1, insideStart);
        in.enterNextPrompts("r", "20221123115442", "");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldCreateRangeQueryWithDefaultStartTime() {
        // Given
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        Instant start = Instant.parse("2022-11-30T07:54:42.000Z");
        CompactionJobStatus insideStart = createFinishedJob("inside-start", tableId,
                start.plusSeconds(1), start.plusSeconds(2));
        createFinishedJob("before-range", tableId, start.minusSeconds(2), start.minusSeconds(1));
        createJob("after-range", tableId, Instant.parse("2022-11-30T11:54:42.000Z").plusSeconds(1));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T08:33:12.001Z"));
        List<CompactionJobStatus> expected = List.of(status2, status1, insideStart);
        in.enterNextPrompts("r", "", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldRepeatRangeQueryPromptWithInvalidStartTime() {
        // Given
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        CompactionJobStatus insideStart = createFinishedJob("inside-start", tableId,
                start.plusSeconds(1), start.plusSeconds(2));
        createFinishedJob("before-range", tableId, start.minusSeconds(2), start.minusSeconds(1));
        createJob("after-range", tableId, Instant.parse("2022-11-30T11:54:42.000Z").plusSeconds(1));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T08:33:12.001Z"));
        List<CompactionJobStatus> expected = List.of(status2, status1, insideStart);
        in.enterNextPrompts("r", "abc", "20221123115442", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldRepeatRangeQueryPromptWithInvalidEndTime() {
        // Given
        Instant end = Instant.parse("2022-11-30T11:54:42.000Z");
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        Instant start = Instant.parse("2022-11-23T11:54:42.000Z");
        CompactionJobStatus insideStart = createFinishedJob("inside-start", tableId,
                start.plusSeconds(1), start.plusSeconds(2));
        createFinishedJob("before-range", tableId, start.minusSeconds(2), start.minusSeconds(1));
        createJob("after-range", tableId, Instant.parse("2022-11-30T11:54:42.000Z").plusSeconds(1));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T08:33:12.001Z"));
        List<CompactionJobStatus> expected = List.of(status2, status1, insideStart);
        in.enterNextPrompts("r", "20221123115442", "abc", "20221130115442");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPromptAtTime(end);

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + RANGE_START_PROMPT + RANGE_END_PROMPT + RANGE_END_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
    }

    @Test
    public void shouldRepeatQueryTypePromptWithInvalidQueryType() {
        // Given
        CompactionJobStatus status1 = createJob("job1", tableId, Instant.parse("2022-11-30T08:33:12.001Z"));
        CompactionJobStatus status2 = createJob("job2", tableId, Instant.parse("2022-11-30T08:53:12.001Z"));
        CompactionJobStatus finished = createFinishedJob("finished-job", tableId,
                Instant.parse("2022-11-30T09:00:00Z"), Instant.parse("2022-11-30T10:00:00Z"));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T09:30:00Z"));
        List<CompactionJobStatus> expected = List.of(finished, status2, status1);
        in.enterNextPrompts("abc", "a");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(out).hasToString(QUERY_TYPE_PROMPT + QUERY_TYPE_PROMPT);
        assertThat(statuses).containsExactlyElementsOf(expected);
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
        return queryStatuses(JobQueryType.PROMPT);
    }

    private List<CompactionJobStatus> queryStatusByPromptAtTime(Instant time) {
        return queryStatusesAtTime(JobQueryType.PROMPT, time);
    }

    private JobQuery queryByPrompt() {
        return queryFrom(JobQueryType.PROMPT);
    }
}
