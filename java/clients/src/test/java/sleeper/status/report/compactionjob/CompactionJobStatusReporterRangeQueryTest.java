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
import sleeper.status.report.CompactionJobStatusReport;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;
import static sleeper.status.report.StatusReporterTestHelper.replaceBracketedJobIds;
import static sleeper.status.report.StatusReporterTestHelper.replaceStandardJobIds;

public class CompactionJobStatusReporterRangeQueryTest extends CompactionJobStatusReporterTestBase {

    @Test
    public void shouldReportCompactionJobStatusForStandardAndSplittingCompactionsInRange() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = mixedJobStatuses();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(replaceStandardJobIds(statusList, example("reports/compactionjobstatus/standard/range/mixedJobs.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/mixedJobs.json")));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoJobsInRange() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = Collections.emptyList();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/noJobs.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json"));
    }

    @Test
    public void shouldConvertInputRangeToUTC() throws Exception {
        // Given
        Instant dateUTC = Instant.parse("2022-09-20T10:00:00.000Z");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("PST"));
        String inputPST = "20220920100000";
        Instant datePST = dateFormat.parse(inputPST).toInstant();

        // When
        Instant parsedDatePST = CompactionJobStatusReport.parseDate(inputPST);

        // Then
        assertThat(parsedDatePST)
                .isEqualTo(dateUTC);
        assertThat(parsedDatePST)
                .isNotEqualTo(datePST);
    }
}
