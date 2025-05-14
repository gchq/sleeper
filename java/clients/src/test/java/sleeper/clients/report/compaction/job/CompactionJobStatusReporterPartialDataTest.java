/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.report.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.clients.report.job.query.JobQuery;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class CompactionJobStatusReporterPartialDataTest extends CompactionJobStatusReporterTestBase {

    List<CompactionJobStatus> statusList = partialJobStatuses();

    @Test
    void shouldReportPartialJobsWithStandardReporter() throws Exception {
        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, JobQuery.Type.ALL))
                .isEqualTo(example("reports/compaction/job/standard/all/partialJobs.txt"));
    }

    @Test
    void shouldReportPartialJobsWithDetailedReporter() throws Exception {
        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, JobQuery.Type.DETAILED))
                .isEqualTo(example("reports/compaction/job/standard/detailed/partialJobs.txt"));
    }

    @Test
    void shouldReportPartialJobsWithJsonReporter() throws Exception {
        // When / Then
        assertThat(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, JobQuery.Type.ALL))
                .isEqualTo(example("reports/compaction/job/json/partialJobs.json"));
    }

}
