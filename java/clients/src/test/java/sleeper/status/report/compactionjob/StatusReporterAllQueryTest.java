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
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;
import static sleeper.compaction.job.CompactionJobTestDataHelper.finishedCompactionStatus;

public class StatusReporterAllQueryTest extends StatusReporterTestBase {

    @Test
    public void shouldReportCompactionJobStatusForStandardAndSplittingCompactions() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = mixedJobStatuses();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(replaceStandardJobIds(statusList, example("reports/compactionjobstatus/standard/all/mixedJobs.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/mixedJobs.json")));
    }

    @Test
    public void shouldReportCompactionJobStatusForMultipleRunsOfSameJob() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = jobWithMultipleRuns();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(replaceStandardJobIds(statusList, example("reports/compactionjobstatus/standard/all/jobWithMultipleRuns.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/jobWithMultipleRuns.json")));
    }

    @Test
    public void shouldReportCompactionJobStatusWithLargeAndDecimalStatistics() throws Exception {
        // Given
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        dataHelper.partitionTree(builder -> builder
                .leavesWithSplits(Arrays.asList(partition("A"), partition("B")), Collections.singletonList("ggg"))
                .parentJoining(partition("C"), partition("A"), partition("B")));
        List<CompactionJobStatus> statusList = Arrays.asList(
                finishedCompactionStatus(
                        dataHelper.singleFileCompaction(partition("C")),
                        Instant.parse("2022-10-13T12:30:00.000Z"),
                        Duration.ofSeconds(60), 1000000, 500000),
                finishedCompactionStatus(
                        dataHelper.singleFileCompaction(partition("C")),
                        Instant.parse("2022-10-13T12:31:00.000Z"),
                        Duration.ofMillis(123), 600, 300),
                finishedCompactionStatus(
                        dataHelper.singleFileSplittingCompaction(partition("C"), partition("A"), partition("B")),
                        Instant.parse("2022-10-13T12:32:00.000Z"),
                        Duration.ofSeconds(60), 1000600, 500300),
                finishedCompactionStatus(
                        dataHelper.singleFileSplittingCompaction(partition("C"), partition("A"), partition("B")),
                        Instant.parse("2022-10-13T12:33:00.000Z"),
                        Duration.ofMillis(123), 1234, 1234));

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(replaceStandardJobIds(statusList, example("reports/compactionjobstatus/standard/all/jobsWithLargeAndDecimalStatistics.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/jobsWithLargeAndDecimalStatistics.json")));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoJobsExist() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = Collections.emptyList();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/standard/all/noJobs.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json"));
    }

}
