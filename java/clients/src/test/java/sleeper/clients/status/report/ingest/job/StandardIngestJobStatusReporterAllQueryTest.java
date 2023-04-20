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

package sleeper.clients.status.report.ingest.job;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.ingest.job.status.IngestJobStatus;

import java.util.Collections;
import java.util.List;

import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.jobWithMultipleRuns;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.jobsWithLargeAndDecimalStatistics;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.mixedJobStatuses;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class StandardIngestJobStatusReporterAllQueryTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        Assertions.assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.ALL, noJobs, 0)).hasToString(
                example("reports/ingest/job/standard/all/noJobs.txt"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobs = mixedJobStatuses();

        // When / Then
        Assertions.assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.ALL, mixedJobs, 2)).hasToString(
                example("reports/ingest/job/standard/all/mixedJobs.txt"));
    }

    @Test
    public void shouldReportIngestJobWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = jobWithMultipleRuns();

        // When / Then
        Assertions.assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.ALL, jobWithMultipleRuns, 0)).hasToString(
                example("reports/ingest/job/standard/all/jobWithMultipleRuns.txt"));
    }

    @Test
    public void shouldReportIngestJobsWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<IngestJobStatus> jobsWithLargeAndDecimalStatistics = jobsWithLargeAndDecimalStatistics();

        // When / Then
        Assertions.assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.ALL, jobsWithLargeAndDecimalStatistics, 0)).hasToString(
                example("reports/ingest/job/standard/all/jobsWithLargeAndDecimalStatistics.txt"));
    }
}
