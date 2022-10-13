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
package sleeper.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.AverageCompactionRate;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobRun;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageCompactionRateTest {

    private static final String DEFAULT_TASK_ID = "test-task-id";
    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    public void shouldCalculateAverageOfSingleFinishedCompactionJob() {
        // Given
        List<CompactionJobStatus> jobs = Collections.singletonList(
                createFinishedCompaction(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedCompactionJobs() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                createFinishedCompaction(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L),
                createFinishedCompaction(
                        Instant.parse("2022-10-13T10:19:00.000Z"),
                        Duration.ofSeconds(10), 50L, 50L));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(2, 7.5, 7.5);
    }

    @Test
    public void shouldIgnoreUnstartedCompactionJob() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                createFinishedCompaction(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L),
                createUnstartedCompaction(
                        Instant.parse("2022-10-13T10:19:00.000Z")));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldIgnoreUnfinishedCompactionJob() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                createFinishedCompaction(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L),
                createUnfinishedCompaction(
                        Instant.parse("2022-10-13T10:19:00.000Z")));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldReportNoCompactionJobs() {
        // Given
        List<CompactionJobStatus> jobs = Collections.emptyList();

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(0, Double.NaN, Double.NaN);
    }

    private CompactionJobStatus createFinishedCompaction(
            Instant createTime, Duration runDuration, long linesRead, long linesWritten) {
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant startTime = createTime.plus(Duration.ofSeconds(10));
        Instant startUpdateTime = startTime.plus(Duration.ofMillis(123));
        Instant finishTime = startTime.plus(runDuration);
        Instant finishUpdateTime = finishTime.plus(Duration.ofMillis(123));
        CompactionJobSummary summary = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(linesRead, linesWritten), startTime, finishTime);
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createTime))
                .singleJobRun(CompactionJobRun.finished(DEFAULT_TASK_ID,
                        CompactionJobStartedStatus.updateAndStartTime(startUpdateTime, startTime),
                        CompactionJobFinishedStatus.updateTimeAndSummary(finishUpdateTime, summary)))
                .build();
    }

    private CompactionJobStatus createUnstartedCompaction(Instant createTime) {
        CompactionJob job = dataHelper.singleFileCompaction();
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createTime))
                .jobRunsLatestFirst(Collections.emptyList())
                .build();
    }

    private CompactionJobStatus createUnfinishedCompaction(Instant createTime) {
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant startTime = createTime.plus(Duration.ofSeconds(10));
        Instant startUpdateTime = startTime.plus(Duration.ofMillis(123));
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createTime))
                .singleJobRun(CompactionJobRun.started(DEFAULT_TASK_ID,
                        CompactionJobStartedStatus.updateAndStartTime(startUpdateTime, startTime)))
                .build();
    }
}
