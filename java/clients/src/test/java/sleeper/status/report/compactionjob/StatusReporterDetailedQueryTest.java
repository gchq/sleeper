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

package sleeper.status.report.compactionjob;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

import java.util.Collections;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class StatusReporterDetailedQueryTest extends StatusReporterTestBase {
    @Test
    public void shouldReportMixedCompactionJobStatus() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = mixedJobStatuses();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/standard/detailed/mixedJobs.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/mixedJobs.json")));
    }

    @Test
    public void shouldReportCompactionJobStatusForMultipleRunsOfSameJob() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = jobWithMultipleRuns();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.DETAILED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/standard/detailed/jobWithMultipleRuns.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.DETAILED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/jobWithMultipleRuns.json")));
    }

    @Test
    public void shouldReportCompactionJobStatusWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = jobsWithLargeAndDecimalStatistics();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.DETAILED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/standard/detailed/jobsWithLargeAndDecimalStatistics.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.DETAILED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/jobsWithLargeAndDecimalStatistics.json")));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoMatchingId() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = Collections.emptyList();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/noJobFound.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json"));

    }
}
