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
import sleeper.compaction.job.status.AverageRecordRate;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageRecordRateTest {

    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    public void shouldCalculateAverageOfSingleFinishedProcess() {
        // Given / When
        AverageRecordRate rate = AverageRecordRate.of(new RecordsProcessedSummary(
                new RecordsProcessed(100L, 100L),
                Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)));

        // Then
        assertThat(rate).extracting(
                        AverageRecordRate::getJobCount,
                        AverageRecordRate::getRecordsReadPerSecond,
                        AverageRecordRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedProcesses() {
        // Given / When
        AverageRecordRate rate = AverageRecordRate.of(
                new RecordsProcessedSummary(
                        new RecordsProcessed(100L, 100L), // rate 10/s
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(10)),
                new RecordsProcessedSummary(
                        new RecordsProcessed(50L, 50L), // rate 5/s
                        Instant.parse("2022-10-13T10:19:00.000Z"), Duration.ofSeconds(10)));

        // Then
        assertThat(rate).extracting(
                        AverageRecordRate::getJobCount,
                        AverageRecordRate::getRecordsReadPerSecond,
                        AverageRecordRate::getRecordsWrittenPerSecond)
                .containsExactly(2, 7.5, 7.5);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedProcessesWithDifferentDurations() {
        // Given / When
        AverageRecordRate rate = AverageRecordRate.of(
                new RecordsProcessedSummary(
                        new RecordsProcessed(1000L, 1000L), // rate 10/s
                        Instant.parse("2022-10-13T10:18:00.000Z"), Duration.ofSeconds(100)),
                new RecordsProcessedSummary(
                        new RecordsProcessed(50L, 50L), // rate 5/s
                        Instant.parse("2022-10-13T10:19:00.000Z"), Duration.ofSeconds(10)));

        // Then
        assertThat(rate)
                .extracting(
                        AverageRecordRate::getJobCount,
                        AverageRecordRate::getRecordsReadPerSecond,
                        AverageRecordRate::getRecordsWrittenPerSecond)
                .containsExactly(2, 7.5, 7.5);
    }

    @Test
    public void shouldReportNoProcesses() {
        // Given
        AverageRecordRate rate = AverageRecordRate.of();

        // When / Then
        assertThat(rate)
                .extracting(
                        AverageRecordRate::getJobCount,
                        AverageRecordRate::getRecordsReadPerSecond,
                        AverageRecordRate::getRecordsWrittenPerSecond)
                .containsExactly(0, Double.NaN, Double.NaN);
    }

}
