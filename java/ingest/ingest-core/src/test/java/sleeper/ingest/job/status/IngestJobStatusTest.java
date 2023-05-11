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

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRunStartedUpdate;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.forJob;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.records;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.withExpiry;
import static sleeper.ingest.job.IngestJobTestData.createJobInDefaultTable;
import static sleeper.ingest.job.status.IngestJobStatusTestData.defaultUpdateTime;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatusListFrom;
import static sleeper.ingest.job.status.IngestJobStatusTestData.singleJobStatusFrom;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestRun;

public class IngestJobStatusTest {
    @Test
    public void shouldBuildAndReportIngestJobStarted() {
        // Given
        IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");

        // When
        IngestJobStatus status = jobStatus(job, startedIngestRun(job, "test-task", startTime));

        // Then
        assertThat(status)
                .extracting(IngestJobStatus::isFinished)
                .isEqualTo(false);
    }

    @Test
    public void shouldBuildAndReportIngestJobFinished() {
        // Given
        IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
        Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);

        // When
        IngestJobStatus status = jobStatus(job, finishedIngestRun(job, "test-task", summary));

        // Then
        assertThat(status)
                .extracting(IngestJobStatus::isFinished)
                .isEqualTo(true);
    }

    @Test
    public void shouldSetExpiryDateFromFirstRecord() {
        IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");
        Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");
        Instant startExpiryTime = Instant.parse("2022-12-21T15:28:42.001Z");
        Instant finishTime = Instant.parse("2022-12-14T15:29:42.001Z");
        Instant finishExpiryTime = Instant.parse("2022-12-21T15:29:42.001Z");
        RecordsProcessedSummary summary = summary(startTime, finishTime, 200, 100);

        IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                forJob(job.getId(), withExpiry(startExpiryTime,
                        IngestJobStartedStatus.startAndUpdateTime(job, startTime, defaultUpdateTime(startTime)))),
                forJob(job.getId(), withExpiry(finishExpiryTime,
                        ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishTime), summary)))));

        assertThat(status.getExpiryDate()).isEqualTo(startExpiryTime);
    }

    @Test
    public void shouldIgnoreJobWithoutStartedUpdateAsItMayHaveExpired() {
        IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");
        Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");
        Instant finishTime = Instant.parse("2022-12-14T15:29:42.001Z");
        RecordsProcessedSummary summary = summary(startTime, finishTime, 200, 100);

        List<IngestJobStatus> statuses = jobStatusListFrom(records().fromUpdates(
                forJob(job.getId(), ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishTime), summary))));

        assertThat(statuses).isEmpty();
    }

    @Test
    void shouldDetectStartOfRunWhenBuildingStartedIngestJobStatusFromStandardIngest() {
        IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");
        Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");

        List<IngestJobStatus> statuses = jobStatusListFrom(records().fromUpdates(
                forJob(job.getId(), IngestJobStartedStatus.startAndUpdateTime(job, startTime, defaultUpdateTime(startTime)))));

        assertThat(statuses)
                .flatMap(IngestJobStatus::getJobRuns)
                .extracting(ProcessRun::getStartedStatus)
                .allMatch(ProcessRunStartedUpdate::isStartOfRun);
    }

    @Test
    void shouldNotDetectStartOfRunWhenBuildingStartedIngestJobStatusFromBulkImport() {
        IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");
        Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");

        List<IngestJobStatus> statuses = jobStatusListFrom(records().fromUpdates(
                forJob(job.getId(), IngestJobStartedStatus.bulkImport()
                        .inputFileCount(job.getFiles().size())
                        .startTime(startTime)
                        .updateTime(defaultUpdateTime(startTime)).build())));

        assertThat(statuses)
                .flatMap(IngestJobStatus::getJobRuns)
                .extracting(ProcessRun::getStartedStatus)
                .allMatch(startedStatus -> !startedStatus.isStartOfRun());
    }
}
