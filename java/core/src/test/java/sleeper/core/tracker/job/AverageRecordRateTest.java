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
package sleeper.core.tracker.job;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.job.status.ProcessFailedStatus;
import sleeper.core.tracker.job.status.ProcessFinishedStatus;
import sleeper.core.tracker.job.status.ProcessRun;
import sleeper.core.tracker.job.status.ProcessStatusUpdateTestHelper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.job.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords.DEFAULT_TASK_ID;

public class AverageRecordRateTest {

    @Test
    public void shouldCalculateAverageOfSingleFinishedProcess() {
        // Given / When
        AverageRecordRate rate = rateFrom(new JobRunSummary(
                new RecordsProcessed(100L, 100L),
                Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)));

        // Then
        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(1, 100L, 100L, Duration.ofSeconds(10), 10.0, 10.0, 10.0, 10.0);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedProcesses() {
        // Given / When
        AverageRecordRate rate = rateFrom(
                new JobRunSummary(
                        new RecordsProcessed(100L, 100L), // rate 10/s
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)),
                new JobRunSummary(
                        new RecordsProcessed(50L, 50L), // rate 5/s
                        Instant.parse("2022-10-13T10:19:00.000Z"), Duration.ofSeconds(10)));

        // Then
        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(2, 150L, 150L, Duration.ofSeconds(20), 7.5, 7.5, 7.5, 7.5);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedProcessesWithDifferentDurations() {
        // Given / When
        AverageRecordRate rate = rateFrom(
                new JobRunSummary(
                        new RecordsProcessed(900L, 900L), // rate 10/s
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(90)),
                new JobRunSummary(
                        new RecordsProcessed(50L, 50L), // rate 5/s
                        Instant.parse("2022-10-13T10:19:00.000Z"), Duration.ofSeconds(10)));

        // Then
        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(2, 950L, 950L, Duration.ofSeconds(100), 9.5, 9.5, 7.5, 7.5);
    }

    @Test
    public void shouldReportNoProcesses() {
        // Given
        AverageRecordRate rate = rateFrom();

        // When / Then
        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(0, 0L, 0L, Duration.ZERO, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    }

    @Test
    public void shouldCalculateWithStartAndEndTimeOutsideOfAnyRuns() {
        // Given / When
        AverageRecordRate rate = AverageRecordRate.builder()
                .startTime(Instant.parse("2022-10-13T10:17:55.000Z"))
                .summary(new JobRunSummary(
                        new RecordsProcessed(100L, 100L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)))
                .finishTime(Instant.parse("2022-10-13T10:18:15.000Z"))
                .build();

        // Then
        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(1, 100L, 100L, Duration.ofSeconds(20), 5.0, 5.0, 10.0, 10.0);
    }

    @Test
    void shouldExcludeARunWithNoRecordsProcessedFromAverageRateCalculation() {
        AverageRecordRate rate = rateFrom(
                new JobRunSummary(
                        new RecordsProcessed(0L, 0L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)),
                new JobRunSummary(
                        new RecordsProcessed(10L, 10L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)));

        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(2, 10L, 10L, Duration.ofSeconds(20), 0.5, 0.5, 1.0, 1.0);
    }

    @Test
    void shouldExcludeARunWithNoRecordsReadFromAverageReadRateCalculation() {
        AverageRecordRate rate = rateFrom(
                new JobRunSummary(
                        new RecordsProcessed(0L, 10L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)),
                new JobRunSummary(
                        new RecordsProcessed(10L, 10L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)));

        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(2, 10L, 20L, Duration.ofSeconds(20), 0.5, 1.0, 1.0, 1.0);
    }

    @Test
    void shouldExcludeARunWithNoRecordsWrittenFromAverageWriteRateCalculation() {
        AverageRecordRate rate = rateFrom(
                new JobRunSummary(
                        new RecordsProcessed(10L, 0L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)),
                new JobRunSummary(
                        new RecordsProcessed(10L, 10L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)));

        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(2, 20L, 10L, Duration.ofSeconds(20), 1.0, 0.5, 1.0, 1.0);
    }

    @Test
    void shouldExcludeARunWithZeroDurationFromAverageRateCalculation() {
        AverageRecordRate rate = rateFrom(
                new JobRunSummary(
                        new RecordsProcessed(10L, 10L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(0)),
                new JobRunSummary(
                        new RecordsProcessed(10L, 10L),
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)));

        assertThat(rate).extracting("runCount", "recordsRead", "recordsWritten", "totalDuration",
                "recordsReadPerSecond", "recordsWrittenPerSecond",
                "averageRunRecordsReadPerSecond", "averageRunRecordsWrittenPerSecond")
                .containsExactly(2, 20L, 20L, Duration.ofSeconds(10), 2.0, 2.0, 1.0, 1.0);
    }

    @Test
    void shouldIgnoreFailedRun() {
        Instant startTime = Instant.parse("2022-10-13T10:18:00.000Z");
        Instant finishTime = Instant.parse("2022-10-13T10:19:00.000Z");

        assertThat(AverageRecordRate.of(Stream.of(
                ProcessRun.finished(DEFAULT_TASK_ID,
                        ProcessStatusUpdateTestHelper.startedStatus(startTime),
                        ProcessFailedStatus.timeAndReasons(defaultUpdateTime(finishTime),
                                new JobRunTime(startTime, finishTime),
                                List.of("Unexpected failure", "Some IO problem"))))))
                .extracting("runCount", "recordsRead", "recordsWritten", "totalDuration")
                .containsExactly(0, 0L, 0L, Duration.ofSeconds(0));
    }

    private static AverageRecordRate rateFrom(JobRunSummary... summaries) {
        return AverageRecordRate.of(Stream.of(summaries)
                .map(summary -> ProcessRun.finished(DEFAULT_TASK_ID,
                        ProcessStatusUpdateTestHelper.startedStatus(summary.getStartTime()),
                        ProcessFinishedStatus.updateTimeAndSummary(summary.getFinishTime(), summary))));
    }

}
