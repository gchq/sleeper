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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.acceptedJob;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.acceptedJobWhichStarted;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.jobWithMultipleRuns;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.jobsWithLargeAndDecimalStatistics;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.mixedJobStatuses;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.rejectedJobWithMultipleReasons;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestHelper.getJsonReport;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class JsonIngestJobStatusReporterAllQueryTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        assertThat(getJsonReport(JobQuery.Type.ALL, noJobs, 0))
                .isEqualTo(example("reports/ingest/job/json/noJobs.json"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobStatuses = mixedJobStatuses();

        // When / Then
        assertThatJson(getJsonReport(JobQuery.Type.ALL, mixedJobStatuses, 0))
                .isEqualTo(example("reports/ingest/job/json/mixedJobs.json"));
    }

    @Test
    public void shouldReportIngestJobsWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = jobWithMultipleRuns();

        // When / Then
        assertThatJson(getJsonReport(JobQuery.Type.ALL, jobWithMultipleRuns, 0))
                .isEqualTo(example("reports/ingest/job/json/jobWithMultipleRuns.json"));
    }

    @Test
    public void shouldReportIngestJobsWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<IngestJobStatus> jobsWithLargeAndDecimalStatistics = jobsWithLargeAndDecimalStatistics();

        // When / Then
        assertThatJson(getJsonReport(JobQuery.Type.ALL, jobsWithLargeAndDecimalStatistics, 0))
                .isEqualTo(example("reports/ingest/job/json/jobsWithLargeAndDecimalStatistics.json"));
    }

    @Test
    public void shouldReportNoIngestJobsWithPersistentEmrStepsNotFinished() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();
        Map<String, Integer> stepCount = Map.of("PENDING", 2, "RUNNING", 1);

        // When / Then
        assertThatJson(getJsonReport(JobQuery.Type.ALL, noJobs, 0, stepCount))
                .isEqualTo(example("reports/ingest/job/json/noJobsWithEmrStepsUnfinished.json"));
    }

    @Nested
    @DisplayName("Bulk Import job reporting")
    class BulkImportJobReporting {
        @Test
        void shouldReportPendingBulkImportJobWithValidationAccepted() throws Exception {
            // Given
            List<IngestJobStatus> acceptedJob = acceptedJob();

            // When / Then
            assertThatJson(getJsonReport(JobQuery.Type.ALL, acceptedJob, 0))
                    .isEqualTo(example("reports/ingest/job/json/bulkImport/acceptedJob.json"));
        }

        @Test
        void shouldReportAcceptedBulkImportJobWhichStarted() throws Exception {
            // Given
            List<IngestJobStatus> acceptedJobWhichStarted = acceptedJobWhichStarted();

            // When / Then
            assertThatJson(getJsonReport(JobQuery.Type.ALL, acceptedJobWhichStarted, 0))
                    .isEqualTo(example("reports/ingest/job/json/bulkImport/acceptedJobWhichStarted.json"));
        }

        @Test
        void shouldReportRejectedJob() throws Exception {
            // Given
            List<IngestJobStatus> rejectedJob = rejectedJobWithMultipleReasons();

            // When / Then
            assertThatJson(getJsonReport(JobQuery.Type.ALL, rejectedJob, 0))
                    .isEqualTo(example("reports/ingest/job/json/bulkImport/rejectedJob.json"));
        }
    }
}
