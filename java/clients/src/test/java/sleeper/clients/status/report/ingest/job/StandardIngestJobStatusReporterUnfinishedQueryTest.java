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
import sleeper.ingest.core.job.status.IngestJobStatus;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class StandardIngestJobStatusReporterUnfinishedQueryTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> noJobs = Collections.emptyList();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.UNFINISHED, noJobs, 0)).hasToString(
                example("reports/ingest/job/standard/unfinished/noJobs.txt"));
    }

    @Test
    public void shouldReportMixedUnfinishedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedUnfinishedJobStatuses = IngestJobStatusReporterTestData.mixedUnfinishedJobStatuses();

        // When / Then
        assertThat(IngestJobStatusReporterTestHelper.getStandardReport(JobQuery.Type.UNFINISHED, mixedUnfinishedJobStatuses, 2)).hasToString(
                example("reports/ingest/job/standard/unfinished/mixedUnfinishedJobs.txt"));
    }
}
