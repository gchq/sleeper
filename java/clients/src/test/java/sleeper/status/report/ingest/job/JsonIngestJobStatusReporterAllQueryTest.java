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

package sleeper.status.report.ingest.job;

import org.junit.Test;
import sleeper.ingest.job.status.IngestJobStatus;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;
import static sleeper.status.report.ingest.job.IngestJobStatusReporter.QueryType;
import static sleeper.status.report.ingest.job.IngestJobStatusReporterTestData.jobWithMultipleRuns;
import static sleeper.status.report.ingest.job.IngestJobStatusReporterTestData.jobsWithLargeAndDecimalStatistics;
import static sleeper.status.report.ingest.job.IngestJobStatusReporterTestData.mixedJobStatuses;
import static sleeper.status.report.ingest.job.IngestJobStatusReporterTestHelper.getJsonReport;
import static sleeper.status.report.ingest.job.IngestJobStatusReporterTestHelper.replaceBracketedJobIds;

public class JsonIngestJobStatusReporterAllQueryTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        assertThat(getJsonReport(QueryType.ALL, noJobs, 0)).hasToString(
                example("reports/ingest/job/json/noJobs.json"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobStatuses = mixedJobStatuses();

        // When / Then
        assertThat(getJsonReport(QueryType.ALL, mixedJobStatuses, 0)).hasToString(
                replaceBracketedJobIds(mixedJobStatuses, example("reports/ingest/job/json/mixedJobs.json")));
    }

    @Test
    public void shouldReportIngestJobsWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = jobWithMultipleRuns();

        // When / Then
        assertThat(getJsonReport(QueryType.ALL, jobWithMultipleRuns, 0)).hasToString(
                replaceBracketedJobIds(jobWithMultipleRuns, example("reports/ingest/job/json/jobWithMultipleRuns.json")));
    }

    @Test
    public void shouldReportIngestJobsWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<IngestJobStatus> jobsWithLargeAndDecimalStatistics = jobsWithLargeAndDecimalStatistics();

        // When / Then
        assertThat(getJsonReport(QueryType.ALL, jobsWithLargeAndDecimalStatistics, 0)).hasToString(
                replaceBracketedJobIds(jobsWithLargeAndDecimalStatistics, example("reports/ingest/job/json/jobsWithLargeAndDecimalStatistics.json")));
    }
}
