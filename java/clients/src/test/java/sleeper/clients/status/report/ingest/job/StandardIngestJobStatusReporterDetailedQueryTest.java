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

package sleeper.clients.status.report.ingest.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.ingest.core.job.IngestJob;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.acceptedJob;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.acceptedJobWhichStarted;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.acceptedStatusUpdate;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.createJob;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.jobWithMultipleRuns;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.jobsWithLargeAndDecimalStatistics;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.mixedJobStatuses;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.rejectedJobWithMultipleReasons;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.rejectedJobWithOneReason;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.rejectedStatusUpdate;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestHelper.getStandardReport;
import static sleeper.clients.testutil.ClientTestUtils.example;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.jobStatusListFrom;
import static sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords.forJobRunOnNoTask;
import static sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords.forNoRunNoTask;
import static sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords.records;

public class StandardIngestJobStatusReporterDetailedQueryTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, noJobs, 0)).isEqualTo(
                example("reports/ingest/job/standard/detailed/noJobFound.txt"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobs = mixedJobStatuses();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, mixedJobs, 0))
                .isEqualTo(example("reports/ingest/job/standard/detailed/mixedJobs.txt"));
    }

    @Test
    public void shouldReportJobWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = jobWithMultipleRuns();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, jobWithMultipleRuns, 0))
                .isEqualTo(example("reports/ingest/job/standard/detailed/jobWithMultipleRuns.txt"));
    }

    @Test
    public void shouldReportJobsWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<IngestJobStatus> jobsWithLargeAndDecimalStatistics = jobsWithLargeAndDecimalStatistics();

        // When / Then
        assertThat(getStandardReport(JobQuery.Type.DETAILED, jobsWithLargeAndDecimalStatistics, 0))
                .isEqualTo(example("reports/ingest/job/standard/detailed/jobsWithLargeAndDecimalStatistics.txt"));
    }

    @Nested
    @DisplayName("Bulk Import job reporting")
    class BulkImportJobReporting {
        @Test
        void shouldReportPendingJobWithValidationAccepted() throws Exception {
            // Given
            List<IngestJobStatus> acceptedJob = acceptedJob();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, acceptedJob, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/acceptedJob.txt"));
        }

        @Test
        void shouldReportStartedJobWithValidationAccepted() throws Exception {
            // Given
            List<IngestJobStatus> acceptedJob = acceptedJobWhichStarted();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, acceptedJob, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/acceptedJobWhichStarted.txt"));
        }

        @Test
        void shouldReportRejectedJobWithOneReason() throws Exception {
            // Given
            List<IngestJobStatus> rejectedJob = rejectedJobWithOneReason();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, rejectedJob, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/rejectedJobWithOneReason.txt"));
        }

        @Test
        void shouldReportRejectedJobWithMultipleReasons() throws Exception {
            // Given
            List<IngestJobStatus> rejectedJob = rejectedJobWithMultipleReasons();

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, rejectedJob, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/rejectedJobWithMultipleReasons.txt"));
        }

        @Test
        void shouldReportJobAcceptedThenRejected() throws Exception {
            // Given
            IngestJob job = createJob(1, 2);
            List<IngestJobStatus> status = jobStatusListFrom(records().fromUpdates(
                    forJobRunOnNoTask(job.getId(), "run-1",
                            acceptedStatusUpdate(job, Instant.parse("2023-06-05T17:20:00Z"))),
                    forNoRunNoTask(job.getId(),
                            rejectedStatusUpdate(job, Instant.parse("2023-06-05T17:30:00Z")))));

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, status, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/acceptedThenRejectedJob.txt"));
        }

        @Test
        void shouldReportRejectedJobWithJsonMessageSaved() throws Exception {
            String json = "{\n" +
                    "\"id\": \"a_unique_id\",\n" +
                    "\"tableName\": \"myTable\",\n" +
                    "\"files\": [\n" +
                    "\"databucket/file1.parquet\"\n" +
                    "]\n" +
                    "}";
            IngestJob job = createJob(1, 2);
            List<IngestJobStatus> status = jobStatusListFrom(records().fromUpdates(
                    rejectedStatusUpdate(job, Instant.parse("2023-06-05T17:20:00Z"), json)));

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, status, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/rejectedWithJson.txt"));
        }

        @Test
        void shouldReportRejectedJobWithInvalidJsonMessageSaved() throws IOException {
            String json = "{";
            IngestJob job = createJob(1, 2);
            List<IngestJobStatus> status = jobStatusListFrom(records().fromUpdates(
                    rejectedStatusUpdate(job, Instant.parse("2023-06-05T17:20:00Z"), json)));

            // When / Then
            assertThat(getStandardReport(JobQuery.Type.DETAILED, status, 0))
                    .isEqualTo(example("reports/ingest/job/standard/detailed/bulkImport/rejectedWithInvalidJson.txt"));
        }
    }
}
