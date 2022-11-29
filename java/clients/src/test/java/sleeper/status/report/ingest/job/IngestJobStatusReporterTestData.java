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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.status.report.StatusReporterTestHelper.job;
import static sleeper.status.report.StatusReporterTestHelper.jobRunFinishedInTask;
import static sleeper.status.report.StatusReporterTestHelper.jobRunStartedInTask;
import static sleeper.status.report.StatusReporterTestHelper.task;

public class IngestJobStatusReporterTestData {
    private IngestJobStatusReporterTestData() {
    }

    public static List<IngestJobStatus> mixedUnfinishedJobStatuses() {
        return mixedJobStatuses().stream()
                .filter(status -> !status.isFinished())
                .collect(Collectors.toList());
    }

    public static List<IngestJobStatus> mixedJobStatuses() {
        IngestJob job1 = createJob(2, 1);
        Instant updateTime1 = Instant.parse("2022-09-18T13:34:12.001Z");
        Instant startTime1 = Instant.parse("2022-09-18T13:34:12.001Z");

        IngestJob job2 = createJob(3, 2);
        Instant updateTime2 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant startTime2 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant finishTime2 = Instant.parse("2022-09-19T13:35:12.001Z");

        IngestJob job3 = createJob(5, 3);
        Instant updateTime3 = Instant.parse("2022-09-21T13:34:12.001Z");
        Instant startTime3 = Instant.parse("2022-09-21T13:34:12.001Z");

        IngestJob job4 = createJob(6, 4);
        Instant updateTime4 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startTime4 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant finishTime4 = Instant.parse("2022-09-22T13:35:12.001Z");

        IngestJobStatus status1 = jobStarted(job1, task(1), startTime1, updateTime1);
        IngestJobStatus status2 = jobFinished(job2, task(1), startTime2, updateTime2, finishTime2);
        IngestJobStatus status3 = jobStarted(job3, task(2), startTime3, updateTime3);
        IngestJobStatus status4 = jobFinished(job4, task(2), startTime4, updateTime4, finishTime4);
        return Arrays.asList(status1, status2, status3, status4);
    }

    public static List<IngestJobStatus> jobWithMultipleRuns() {
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

    public static List<IngestJobStatus> jobsWithLargeAndDecimalStatistics() {
        Instant updateTime1 = Instant.parse("2022-10-13T10:01:00Z");
        Instant startTime1 = Instant.parse("2022-10-13T10:00:10.000Z");
        Instant finishTime1 = Instant.parse("2022-10-13T10:00:10.123Z");

        Instant updateTime2 = Instant.parse("2022-10-13T12:01:00Z");
        Instant startTime2 = Instant.parse("2022-10-13T12:01:10Z");
        Instant finishTime2 = Instant.parse("2022-10-13T14:01:10Z");

        Instant updateTime3 = Instant.parse("2022-10-12T10:01:00.000Z");
        Instant startTime3 = Instant.parse("2022-10-13T14:01:10Z");
        Instant finishTime3 = Instant.parse("2022-10-13T14:02:10Z");

        Instant updateTime4 = Instant.parse("2022-10-13T14:02:00.000Z");
        Instant startTime4 = Instant.parse("2022-10-13T14:02:10.000Z");
        Instant finishTime4 = Instant.parse("2022-10-13T14:02:10.123Z");

        return Arrays.asList(
                jobFinished(createJob(1, 1), "task-id", startTime1, updateTime1, finishTime1, 600L, 300L),
                jobFinished(createJob(2, 1), "task-id", startTime2, updateTime2, finishTime2, 1_000_600L, 500_300L),
                jobFinished(createJob(3, 1), "task-id", startTime3, updateTime3, finishTime3, 1_000_600L, 500_300L),
                jobFinished(createJob(4, 1), "task-id", startTime4, updateTime4, finishTime4, 1234L, 1234L));
    }

    private static IngestJob createJob(int jobNum, int inputFileCount) {
        return IngestJob.builder()
                .id(job(jobNum))
                .files(IntStream.range(1, inputFileCount + 1)
                        .mapToObj(f -> String.format("test%1d.parquet", f))
                        .collect(Collectors.toList()))
                .build();
    }

    private static IngestJobStatus jobStarted(IngestJob job, String taskId, Instant startTime, Instant startUpdateTime) {
        return IngestJobStatus.from(job)
                .jobRun(ProcessRun.started(taskId,
                        ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime)))
                .build();
    }

    private static IngestJobStatus jobFinished(IngestJob job, String taskId, Instant startTime, Instant startUpdateTime, Instant finishTime) {
        return jobFinished(job, taskId, startTime, startUpdateTime, finishTime, 600L, 300L);
    }

    private static IngestJobStatus jobFinished(IngestJob job, String taskId, Instant startTime, Instant startUpdateTime, Instant finishTime,
                                               long linesRead, long linesWritten) {
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(linesRead, linesWritten), startTime, finishTime);
        return IngestJobStatus.from(job)
                .jobRun(ProcessRun.finished(taskId,
                        ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime, summary)))
                .build();
    }
}
