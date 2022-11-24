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

package sleeper.ingest.job.status;

import org.junit.Test;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestJobStatusTest {
    @Test
    public void shouldBuildIngestJobStartedFromJob() {
        // Given
        IngestJob job = IngestJob.builder()
                .files("test.parquet", "test2.parquet")
                .id("test-job")
                .build();
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startTime = Instant.parse("2022-09-22T13:33:00.001Z");

        // When
        IngestJobStatus status = IngestJobStatus.started(job, "test-task", updateTime, startTime);

        // Then
        assertThat(status)
                .extracting(IngestJobStatus::getJobId, IngestJobStatus::getInputFileCount, IngestJobStatus::getJobRuns)
                .containsExactly("test-job", 2,
                        Collections.singletonList(ProcessRun.started("test-task",
                                ProcessStartedStatus.updateAndStartTime(updateTime, startTime))));
    }

    @Test
    public void shouldBuildAndReportIngestJobStarted() {
        // Given
        IngestJob job = IngestJob.builder()
                .files("test.parquet", "test2.parquet")
                .id("test-job")
                .build();
        Instant updateTime = Instant.parse("2022-09-22T13:34:00.001Z");
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");

        // When
        IngestJobStatus status = IngestJobStatus.started(job, "test-task", updateTime, startTime);

        // Then
        assertThat(status)
                .extracting(IngestJobStatus::isFinished)
                .isEqualTo(false);
    }

    @Test
    public void shouldBuildAndReportIngestJobFinished() {
        // Given
        IngestJob job = IngestJob.builder()
                .files("test.parquet", "test2.parquet")
                .id("test-job")
                .build();
        Instant updateTime = Instant.parse("2022-09-22T13:34:00.001Z");
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
        Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);

        // When
        IngestJobStatus status = IngestJobStatus.builder()
                .jobId(job.getId())
                .inputFileCount(job.getFiles().size())
                .jobRun(ProcessRun.finished("test-task",
                        ProcessStartedStatus.updateAndStartTime(updateTime, startTime),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime, summary)))
                .build();

        // Then
        assertThat(status)
                .extracting(IngestJobStatus::isFinished)
                .isEqualTo(true);
    }
}
