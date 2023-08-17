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

import org.junit.jupiter.api.Test;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;

public class IngestJobStatusInPeriodTest {
    private final IngestJob job = IngestJob.builder()
            .id("test-job").files("test.parquet").tableName("test-table").build();

    @Test
    public void shouldIncludeUnfinishedJobWhenStartedBeforeWindowStartTime() {
        // Given
        Instant jobStartTime = Instant.parse("2022-09-23T11:43:00.000Z");
        Instant windowStartTime = Instant.parse("2022-09-23T11:44:00.000Z");
        Instant windowEndTime = Instant.parse("2022-09-23T11:44:02.000Z");
        IngestJobStatus status = startedIngestJob(job, "test-task-id", jobStartTime);

        // When / Then
        assertThat(status.isInPeriod(windowStartTime, windowEndTime)).isTrue();
    }
}
