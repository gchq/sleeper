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

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.*;
import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;
import static sleeper.ingest.job.IngestJobTestData.createJobInDefaultTable;
import static sleeper.ingest.job.status.IngestJobStatusTestData.*;

public class IngestJobStatusTest {
    private final IngestJob job = createJobInDefaultTable("test-job", "test.parquet", "test2.parquet");

    @Nested
    @DisplayName("Report when a job is finished")
    class ReportFinished {
        @Test
        public void shouldBuildAndReportIngestJobStarted() {
            // Given
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
            Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
            Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");

            // When
            IngestJobStatus status = jobStatus(job,
                    finishedIngestRun(job, "test-task", summary(startTime, finishTime)));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::isFinished)
                    .isEqualTo(true);
        }
    }

    @Nested
    @DisplayName("Report furthest status update")
    class ReportFurthestUpdate {
        @Test
        void shouldReportValidatedWithOneRun() {
            IngestJobAcceptedStatus validation = acceptedStatusUpdate(Instant.parse("2022-09-22T13:33:10.001Z"));

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("test-run", validation)));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestStatusUpdate)
                    .isEqualTo(validation);
        }

        @Test
        void shouldReportStartedWithOneRun() {
            IngestJobAcceptedStatus validation = acceptedStatusUpdate(Instant.parse("2022-09-22T13:33:10Z"));
            IngestJobStartedStatus started = startedStatusUpdateAfterValidation(Instant.parse("2022-09-22T13:33:11Z"));

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forRunOnNoTask("run", validation),
                    forRunOnTask("run", "task", started)));

            // Then
            assertThat(status)
                    .extracting(IngestJobStatus::getFurthestStatusUpdate)
                    .isEqualTo(started);
        }
    }

    @Nested
    @DisplayName("Apply expiry date")
    class ApplyExpiry {

        @Test
        public void shouldSetExpiryDateFromFirstRecord() {
            Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");
            Instant startExpiryTime = Instant.parse("2022-12-21T15:28:42.001Z");
            Instant finishTime = Instant.parse("2022-12-14T15:29:42.001Z");
            Instant finishExpiryTime = Instant.parse("2022-12-21T15:29:42.001Z");

            IngestJobStatus status = singleJobStatusFrom(records().fromUpdates(
                    forJob(job.getId(), withExpiry(startExpiryTime,
                            startedStatusUpdate(startTime))),
                    forJob(job.getId(), withExpiry(finishExpiryTime,
                            finishedStatusUpdate(startTime, finishTime)))));

            assertThat(status.getExpiryDate()).isEqualTo(startExpiryTime);
        }

        @Test
        public void shouldIgnoreJobWithoutStartedUpdateAsItMayHaveExpired() {
            Instant startTime = Instant.parse("2022-12-14T15:28:42.001Z");
            Instant finishTime = Instant.parse("2022-12-14T15:29:42.001Z");

            List<IngestJobStatus> statuses = jobStatusListFrom(records().fromUpdates(
                    forJob(job.getId(), finishedStatusUpdate(startTime, finishTime))));

            assertThat(statuses).isEmpty();
        }
    }

    private IngestJobAcceptedStatus acceptedStatusUpdate(Instant validationTime) {
        return IngestJobAcceptedStatus.from(job, validationTime, defaultUpdateTime(validationTime));
    }

    private IngestJobStartedStatus startedStatusUpdate(Instant startTime) {
        return IngestJobStartedStatus.startAndUpdateTime(job, startTime, defaultUpdateTime(startTime));
    }

    private IngestJobStartedStatus startedStatusUpdateAfterValidation(Instant startTime) {
        return IngestJobStartedStatus.withStartOfRun(false).job(job)
                .startTime(startTime).updateTime(defaultUpdateTime(startTime))
                .build();
    }

    private ProcessFinishedStatus finishedStatusUpdate(Instant startTime, Instant finishTime) {
        return ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(finishTime), summary(startTime, finishTime));
    }

    private RecordsProcessedSummary summary(Instant startTime, Instant finishTime) {
        return new RecordsProcessedSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);
    }
}
