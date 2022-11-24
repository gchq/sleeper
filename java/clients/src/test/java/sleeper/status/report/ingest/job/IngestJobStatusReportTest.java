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
import sleeper.ToStringPrintStream;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class IngestJobStatusReportTest {
    @Test
    public void shouldReportNoIngestJobs() throws Exception {

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.ALL)).hasToString(
                example("reports/ingest/job/standard/all/noJobs.txt"));
    }

    @Test
    public void shouldReportMixedIngestJobs() throws Exception {
        // Given
        List<IngestJobStatus> mixedJobs = createMixedJobs();

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.ALL, mixedJobs, 2)).hasToString(
                example("reports/ingest/job/standard/all/mixedJobs.txt"));
    }


    private String getStandardReport(IngestJobQuery query) {
        return getStandardReport(query, Collections.emptyList(), 0);
    }

    private String getStandardReport(IngestJobQuery query, List<IngestJobStatus> statusList, int numberInQueue) {
        ToStringPrintStream output = new ToStringPrintStream();
        new IngestJobStatusReport(output.getPrintStream()).run(query, statusList, numberInQueue);
        return output.toString();
    }

    private List<IngestJobStatus> createMixedJobs() {
        IngestJob job1 = IngestJob.builder()
                .files("test11.parquet")
                .id("job22222-2222-2222-2222-222222222222")
                .build();
        Instant updateTime1 = Instant.parse("2022-09-18T13:34:12.001Z");
        Instant startTime1 = Instant.parse("2022-09-18T13:34:12.001Z");

        IngestJob job2 = IngestJob.builder()
                .files("test22.parquet", "test22.parquet")
                .id("job33333-3333-3333-3333-333333333333")
                .build();
        Instant updateTime2 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant startTime2 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant finishTime2 = Instant.parse("2022-09-19T13:35:12.001Z");
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(600L, 300L), startTime2, finishTime2);

        IngestJob job3 = IngestJob.builder()
                .files("test31.parquet", "test32.parquet", "test33.parquet")
                .id("job55555-5555-5555-5555-555555555555")
                .build();
        Instant updateTime3 = Instant.parse("2022-09-21T13:34:12.001Z");
        Instant startTime3 = Instant.parse("2022-09-21T13:34:12.001Z");

        IngestJob job4 = IngestJob.builder()
                .files("test41.parquet", "test42.parquet", "test43.parquet", "test44.parquet")
                .id("job66666-6666-6666-6666-666666666666")
                .build();
        Instant updateTime4 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startTime4 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant finishTime4 = Instant.parse("2022-09-22T13:35:12.001Z");
        RecordsProcessedSummary summary4 = new RecordsProcessedSummary(
                new RecordsProcessed(600L, 300L), startTime4, finishTime4);
        // When
        IngestJobStatus status1 = IngestJobStatus.builder()
                .jobId(job1.getId())
                .inputFileCount(job1.getFiles().size())
                .jobRun(ProcessRun.started("task1111-1111-1111-1111-111111111111",
                        ProcessStartedStatus.updateAndStartTime(updateTime1, startTime1)))
                .build();
        IngestJobStatus status2 = IngestJobStatus.builder()
                .jobId(job2.getId())
                .inputFileCount(job2.getFiles().size())
                .jobRun(ProcessRun.finished("task1111-1111-1111-1111-111111111111",
                        ProcessStartedStatus.updateAndStartTime(updateTime2, startTime2),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime2, summary2)))
                .build();
        IngestJobStatus status3 = IngestJobStatus.builder()
                .jobId(job3.getId())
                .inputFileCount(job3.getFiles().size())
                .jobRun(ProcessRun.started("task2222-2222-2222-2222-222222222222",
                        ProcessStartedStatus.updateAndStartTime(updateTime3, startTime3)))
                .build();
        IngestJobStatus status4 = IngestJobStatus.builder()
                .jobId(job4.getId())
                .inputFileCount(job4.getFiles().size())
                .jobRun(ProcessRun.finished("task2222-2222-2222-2222-222222222222",
                        ProcessStartedStatus.updateAndStartTime(updateTime4, startTime4),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime4, summary4)))
                .build();
        return Arrays.asList(status1, status2, status3, status4);
    }
}
