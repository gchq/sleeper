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

import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.core.tracker.ingest.job.IngestJobStatus;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.rejectedJobWithOneReason;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class StandardIngestJobStatusReporterRejectedQueryTest {
    @Test
    void shouldReportOneRejectedJob() throws Exception {
        // Given
        List<IngestJobStatus> statusList = rejectedJobWithOneReason();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.REJECTED, statusList, 0)).hasToString(
                example("reports/ingest/job/standard/rejected/rejectedJob.txt"));
    }

    @Test
    void shouldReportNoRejectedJobs() throws Exception {
        // Given
        List<IngestJobStatus> statusList = Collections.emptyList();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.REJECTED, statusList, 0)).hasToString(
                example("reports/ingest/job/standard/rejected/noJobs.txt"));
    }
}
