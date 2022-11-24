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

public class IngestJobStatusReportDetailedQueryTest extends IngestJobStatusReporterTestBase {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.DETAILED, noJobs, 0)).hasToString(
                example("reports/ingest/job/standard/detailed/noJobFound.txt"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobs = mixedJobStatuses();

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.DETAILED, mixedJobs, 0)).hasToString(
                replaceBracketedJobIds(mixedJobs, example("reports/ingest/job/standard/detailed/mixedJobs.txt")));
    }

    @Test
    public void shouldReportJobWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = jobWithMultipleRuns();

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.DETAILED, jobWithMultipleRuns, 0)).hasToString(
                replaceBracketedJobIds(jobWithMultipleRuns, example("reports/ingest/job/standard/detailed/jobWithMultipleRuns.txt")));
    }
}
