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

package sleeper.ingest.job.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;

public class IngestJobStatusInPeriodTest {
    private final IngestJob job = IngestJob.builder()
            .id("test-job").files("test.parquet").tableName("test-table").build();

    @Nested
    @DisplayName("Unfinished job")
    class UnfinishedJob {

        @Test
        public void shouldIncludeUnfinishedJobWhenStartedBeforeWindowStartTime() {
            // Given
            Instant jobStartTime = Instant.parse("2022-09-23T11:43:00.000Z");
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(unfinishedStatus(jobStartTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }

        @Test
        public void shouldIncludeUnfinishedJobWhenStartedDuringWindow() {
            // Given
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant jobStartTime = Instant.parse("2022-09-23T11:44:30.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");

            // When / Then
            assertThat(unfinishedStatus(jobStartTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isTrue();
        }

        @Test
        public void shouldNotIncludeUnfinishedJobWhenStartedAfterWindow() {
            // Given
            Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
            Instant windowEndTime = Instant.parse("2022-09-23T11:45:00.000Z");
            Instant jobStartTime = Instant.parse("2022-09-23T11:45:30.000Z");

            // When / Then
            assertThat(unfinishedStatus(jobStartTime)
                    .isInPeriod(windowStartTime, windowEndTime))
                    .isFalse();
        }
    }

    private IngestJobStatus unfinishedStatus(Instant startTime) {
        return startedIngestJob(job, "test-task-id", startTime);
    }
}
