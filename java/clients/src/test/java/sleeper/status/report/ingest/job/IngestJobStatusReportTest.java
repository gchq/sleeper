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
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.ProcessStartedStatus;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;
import static sleeper.status.report.StatusReporterTestHelper.job;
import static sleeper.status.report.StatusReporterTestHelper.jobRunFinishedInTask;
import static sleeper.status.report.StatusReporterTestHelper.jobRunStartedInTask;

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

    @Test
    public void shouldReportIngestJobWithMultipleRuns() throws Exception {
        // Given
        List<IngestJobStatus> jobWithMultipleRuns = createJobsWithMultipleRuns();

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.ALL, jobWithMultipleRuns, 0)).hasToString(
                example("reports/ingest/job/standard/all/jobWithMultipleRuns.txt"));
    }

    @Test
    public void shouldReportIngestJobsWithLargeAndDecimalStatistics() throws Exception {
        // Given
        List<IngestJobStatus> jobsWithLargeAndDecimalStatistics = createJobsWithLargeAndDecimalStatistics();

        // When / Then
        assertThat(getStandardReport(IngestJobQuery.ALL, jobsWithLargeAndDecimalStatistics, 0)).hasToString(
                example("reports/ingest/job/standard/all/jobsWithLargeAndDecimalStatistics.txt"));
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
        IngestJobStatus status1 = IngestJobStatus.from(job1)
                .jobRun(ProcessRun.started("task1111-1111-1111-1111-111111111111",
                        ProcessStartedStatus.updateAndStartTime(updateTime1, startTime1)))
                .build();
        IngestJobStatus status2 = IngestJobStatus.from(job2)
                .jobRun(ProcessRun.finished("task1111-1111-1111-1111-111111111111",
                        ProcessStartedStatus.updateAndStartTime(updateTime2, startTime2),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime2, summary2)))
                .build();
        IngestJobStatus status3 = IngestJobStatus.from(job3)
                .jobRun(ProcessRun.started("task2222-2222-2222-2222-222222222222",
                        ProcessStartedStatus.updateAndStartTime(updateTime3, startTime3)))
                .build();
        IngestJobStatus status4 = IngestJobStatus.from(job4)
                .jobRun(ProcessRun.finished("task2222-2222-2222-2222-222222222222",
                        ProcessStartedStatus.updateAndStartTime(updateTime4, startTime4),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime4, summary4)))
                .build();
        return Arrays.asList(status1, status2, status3, status4);
    }

    private List<IngestJobStatus> createJobsWithMultipleRuns() {
        List<ProcessRun> processRuns = Arrays.asList(
                jobRunStartedInTask(1, "2022-10-12T10:02:00"),
                jobRunFinishedInTask(2, "2022-10-12T10:01:15", "2022-10-12T10:01:45"),
                jobRunFinishedInTask(1, "2022-10-12T10:01:00", "2022-10-12T10:01:20"));

        IngestJobStatus status = IngestJobStatus.builder()
                .jobId(job(2))
                .inputFileCount(1)
                .jobRuns(ProcessRuns.latestFirst(processRuns))
                .build();
        return Collections.singletonList(status);
    }

    private List<IngestJobStatus> createJobsWithLargeAndDecimalStatistics() {
        Instant updateTime1 = Instant.parse("2022-10-13T12:01:00Z");
        Instant startTime1 = Instant.parse("2022-10-13T10:00:10.000Z");
        Instant finishTime1 = Instant.parse("2022-10-13T10:00:10.123Z");
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(600L, 300L), startTime1, finishTime1);

        Instant updateTime2 = Instant.parse("2022-10-13T12:01:00Z");
        Instant startTime2 = Instant.parse("2022-10-13T12:01:10Z");
        Instant finishTime2 = Instant.parse("2022-10-13T14:01:10Z");
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(1_000_600L, 500_300L), startTime2, finishTime2);
        Instant updateTime3 = Instant.parse("2022-10-12T10:01:00.000Z");
        Instant startTime3 = Instant.parse("2022-10-13T14:01:10Z");
        Instant finishTime3 = Instant.parse("2022-10-13T14:02:10Z");
        RecordsProcessedSummary summary3 = new RecordsProcessedSummary(
                new RecordsProcessed(1_000_600L, 500_300L), startTime3, finishTime3);

        Instant updateTime4 = Instant.parse("2022-10-13T14:02:00.000Z");
        Instant startTime4 = Instant.parse("2022-10-13T14:02:10.000Z");
        Instant finishTime4 = Instant.parse("2022-10-13T14:02:10.123Z");
        RecordsProcessedSummary summary4 = new RecordsProcessedSummary(
                new RecordsProcessed(1234L, 1234L), startTime4, finishTime4);

        return Arrays.asList(
                IngestJobStatus.builder()
                        .jobId("job11111-1111-1111-1111-111111111111")
                        .inputFileCount(1)
                        .jobRun(ProcessRun.finished("task-id",
                                ProcessStartedStatus.updateAndStartTime(updateTime1, startTime1),
                                ProcessFinishedStatus.updateTimeAndSummary(finishTime1, summary1)))
                        .build(),
                IngestJobStatus.builder()
                        .jobId("job22222-2222-2222-2222-222222222222")
                        .inputFileCount(1)
                        .jobRun(ProcessRun.finished("task-id",
                                ProcessStartedStatus.updateAndStartTime(updateTime2, startTime2),
                                ProcessFinishedStatus.updateTimeAndSummary(finishTime2, summary2)))
                        .build(),
                IngestJobStatus.builder()
                        .jobId("job33333-3333-3333-3333-333333333333")
                        .inputFileCount(1)
                        .jobRun(ProcessRun.finished("task-id",
                                ProcessStartedStatus.updateAndStartTime(updateTime3, startTime3),
                                ProcessFinishedStatus.updateTimeAndSummary(finishTime3, summary3)))
                        .build(),
                IngestJobStatus.builder()
                        .jobId("job44444-4444-4444-4444-444444444444")
                        .inputFileCount(1)
                        .jobRun(ProcessRun.finished("task-id",
                                ProcessStartedStatus.updateAndStartTime(updateTime4, startTime4),
                                ProcessFinishedStatus.updateTimeAndSummary(finishTime4, summary4)))
                        .build());
    }
}
