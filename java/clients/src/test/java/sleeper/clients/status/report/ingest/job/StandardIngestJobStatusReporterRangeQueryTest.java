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

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.mixedJobStatuses;
import static sleeper.clients.status.report.ingest.job.IngestJobStatusReporterTestData.mixedUnfinishedJobStatuses;
import static sleeper.clients.testutil.ClientTestUtils.example;

class StandardIngestJobStatusReporterRangeQueryTest {
    @Test
    void shouldReportMixedIngestJobs() throws IOException {
        // Given
        List<IngestJobStatus> mixedJobs = mixedJobStatuses();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.RANGE, mixedJobs, 2))
                .isEqualTo(example("reports/ingest/job/standard/range/mixedJobs.txt"));
    }

    @Test
    void shouldReportMixedUnfinishedIngestJobs() throws IOException {
        // Given
        List<IngestJobStatus> mixedJobs = mixedUnfinishedJobStatuses();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.RANGE, mixedJobs, 2))
                .isEqualTo(example("reports/ingest/job/standard/range/unfinishedJobs.txt"));
    }

    @Test
    void shouldReportNoIngestJobs() throws IOException {
        // Given
        List<IngestJobStatus> noJobs = List.of();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.RANGE, noJobs, 0))
                .isEqualTo(example("reports/ingest/job/standard/range/noJobs.txt"));
    }
}
