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
package sleeper.core.tracker.job.run;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class JobRunSummaryTest {

    @Test
    public void shouldReturnInputs() {
        long recordsRead = 100L;
        long recordsWritten = 100L;
        Instant startTime = Instant.parse("2022-09-22T09:44:00.000Z");
        Instant finishTime = Instant.parse("2022-09-22T09:45:00.000Z");
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(recordsRead, recordsWritten),
                startTime, finishTime);

        assertThat(summary).extracting("recordsRead", "recordsWritten", "startTime", "finishTime")
                .containsExactly(recordsRead, recordsWritten, startTime, finishTime);
    }

    @Test
    public void shouldCalculateDuration() {
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(100L, 100L),
                Instant.parse("2022-09-22T09:44:00.000Z"),
                Instant.parse("2022-09-22T09:45:00.000Z"));

        assertThat(summary.getDurationInSeconds()).isEqualTo(60.0);
    }

    @Test
    public void shouldCalculateRecordRate() {
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(450L, 300L),
                Instant.parse("2022-09-22T09:44:00.000Z"),
                Instant.parse("2022-09-22T09:45:00.000Z"));

        assertThat(summary).extracting("recordsReadPerSecond", "recordsWrittenPerSecond")
                .containsExactly(7.5, 5.0);
    }
}
