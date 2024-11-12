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

package sleeper.clients.status.report.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.compaction.core.job.status.CompactionJobStatus;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class CompactionJobStatusReporterUnfinishedQueryTest extends CompactionJobStatusReporterTestBase {

    @Test
    public void shouldReportMixedCompactionJobStatusForUnfinishedJobs() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = mixedUnfinishedJobStatuses();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, JobQuery.Type.UNFINISHED))
                .isEqualTo(example("reports/compaction/job/standard/unfinished/mixedUnfinishedJobs.txt"));
        assertThat(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, JobQuery.Type.UNFINISHED))
                .isEqualTo(example("reports/compaction/job/json/mixedUnfinishedJobs.json"));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoJobsUnfinished() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = Collections.emptyList();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, JobQuery.Type.UNFINISHED))
                .isEqualTo(example("reports/compaction/job/standard/unfinished/noJobs.txt"));
        assertThat(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, JobQuery.Type.UNFINISHED))
                .isEqualTo(example("reports/compaction/job/json/noJobs.json"));
    }
}
