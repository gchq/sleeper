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

package sleeper.status.report.compaction.job;

import sleeper.ToStringPrintStream;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusTestData;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.status.report.StatusReporterTestHelper;
import sleeper.status.report.job.query.JobQuery.Type;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.ClientTestUtils.exampleUUID;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.compaction.job.CompactionJobTestDataHelper.finishedCompactionStatus;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.status.report.StatusReporterTestHelper.task;

public abstract class CompactionJobStatusReporterTestBase {

    protected static String partition(String letter) {
        return exampleUUID("partn", letter);
    }

    protected static List<CompactionJobStatus> mixedJobStatuses() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        dataHelper.partitionTree(builder -> builder
                .leavesWithSplits(
                        Arrays.asList(partition("A"), partition("B"), partition("C"), partition("D")),
                        Arrays.asList("ddd", "ggg", "kkk"))
                .parentJoining(partition("E"), partition("A"), partition("B"))
                .parentJoining(partition("F"), partition("E"), partition("C"))
                .parentJoining(partition("G"), partition("F"), partition("D")));

        CompactionJob job1 = dataHelper.singleFileCompaction(partition("A"));
        Instant creationTime1 = Instant.parse("2022-09-17T13:33:12.001Z");
        CompactionJob job2 = dataHelper.singleFileCompaction(partition("B"));
        Instant creationTime2 = Instant.parse("2022-09-18T13:33:12.001Z");
        Instant startedTime2 = Instant.parse("2022-09-18T13:34:12.001Z");
        Instant startedUpdateTime2 = Instant.parse("2022-09-18T13:39:12.001Z");
        CompactionJob job3 = dataHelper.singleFileCompaction(partition("C"));
        Instant creationTime3 = Instant.parse("2022-09-19T13:33:12.001Z");
        Instant startedTime3 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant startedUpdateTime3 = Instant.parse("2022-09-19T13:39:12.001Z");
        Instant finishedTime3 = Instant.parse("2022-09-19T13:40:12.001Z");

        CompactionJob job4 = dataHelper.singleFileSplittingCompaction(partition("E"), partition("A"), partition("B"));
        Instant creationTime4 = Instant.parse("2022-09-20T13:33:12.001Z");
        CompactionJob job5 = dataHelper.singleFileSplittingCompaction(partition("F"), partition("E"), partition("C"));
        Instant creationTime5 = Instant.parse("2022-09-21T13:33:12.001Z");
        Instant startedTime5 = Instant.parse("2022-09-21T13:34:12.001Z");
        Instant startedUpdateTime5 = Instant.parse("2022-09-21T13:39:12.001Z");
        CompactionJob job6 = dataHelper.singleFileSplittingCompaction(partition("G"), partition("F"), partition("D"));
        Instant creationTime6 = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime6 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime6 = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime6 = Instant.parse("2022-09-22T13:40:12.001Z");

        CompactionJobStatus status1 = jobCreated(job1, creationTime1);
        CompactionJobStatus status2 = jobStarted(job2, task(1), creationTime2, startedTime2, startedUpdateTime2);
        CompactionJobStatus status3 = jobFinished(job3, task(1), creationTime3, startedTime3, startedUpdateTime3, finishedTime3);
        CompactionJobStatus status4 = jobCreated(job4, creationTime4);
        CompactionJobStatus status5 = jobStarted(job5, task(2), creationTime5, startedTime5, startedUpdateTime5);
        CompactionJobStatus status6 = jobFinished(job6, task(2), creationTime6, startedTime6, startedUpdateTime6, finishedTime6);
        return Arrays.asList(status6, status5, status4, status3, status2, status1);
    }

    protected static List<CompactionJobStatus> mixedUnfinishedJobStatuses() {
        return mixedJobStatuses().stream()
                .filter(job -> !job.isFinished())
                .collect(Collectors.toList());
    }

    protected static List<CompactionJobStatus> jobWithMultipleRuns() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

        CompactionJob job = dataHelper.singleFileCompaction();
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, Instant.parse("2022-10-12T10:00:00.001Z")))
                .jobRunsLatestFirst(Arrays.asList(
                        startedCompactionRun(task(1), Instant.parse("2022-10-12T10:02:00.001Z")),
                        finishedCompactionRun(task(2), summary(
                                Instant.parse("2022-10-12T10:01:15.001Z"),
                                Duration.ofSeconds(30), 300L, 200L)),
                        finishedCompactionRun(task(1), summary(
                                Instant.parse("2022-10-12T10:01:00.001Z"),
                                Duration.ofSeconds(20), 300L, 200L))))
                .build();
        return Collections.singletonList(status);
    }

    protected static List<CompactionJobStatus> jobsWithLargeAndDecimalStatistics() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        dataHelper.partitionTree(builder -> builder
                .leavesWithSplits(Arrays.asList(partition("A"), partition("B")), Collections.singletonList("ggg"))
                .parentJoining(partition("C"), partition("A"), partition("B")));
        return Arrays.asList(
                finishedCompactionStatus(
                        dataHelper.singleFileSplittingCompaction(partition("C"), partition("A"), partition("B")),
                        "task-id",
                        Instant.parse("2022-10-13T14:02:00.000Z"),
                        Duration.ofMillis(123), 1234, 1234),
                finishedCompactionStatus(
                        dataHelper.singleFileSplittingCompaction(partition("C"), partition("A"), partition("B")),
                        "task-id",
                        Instant.parse("2022-10-13T14:01:00.000Z"),
                        Duration.ofSeconds(60), 1000600, 500300),
                finishedCompactionStatus(
                        dataHelper.singleFileCompaction(partition("C")),
                        "task-id",
                        Instant.parse("2022-10-13T12:01:00.000Z"),
                        Duration.ofHours(2), 1000600, 500300),
                finishedCompactionStatus(
                        dataHelper.singleFileCompaction(partition("C")),
                        "task-id",
                        Instant.parse("2022-10-13T12:00:00.000Z"),
                        Duration.ofMillis(123), 600, 300));
    }

    protected static CompactionJobStatus jobCreated(CompactionJob job, Instant creationTime) {
        return CompactionJobStatusTestData.jobCreated(job, creationTime);
    }

    protected static CompactionJobStatus jobStarted(CompactionJob job, String taskId, Instant creationTime, Instant startTime, Instant startUpdateTime) {
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, creationTime))
                .singleJobRun(ProcessRun.started(taskId, CompactionJobStartedStatus.startAndUpdateTime(startTime, startUpdateTime)))
                .build();
    }

    protected static CompactionJobStatus jobFinished(CompactionJob job, String taskId, Instant creationTime, Instant startTime, Instant startUpdateTime, Instant finishedTime) {
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(600L, 300L), startUpdateTime, finishedTime);
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, creationTime))
                .singleJobRun(ProcessRun.finished(taskId, CompactionJobStartedStatus.startAndUpdateTime(startTime, startUpdateTime),
                        ProcessFinishedStatus.updateTimeAndSummary(finishedTime, summary)))
                .build();
    }

    public static String replaceStandardJobIds(List<CompactionJobStatus> job, String example) {
        return StatusReporterTestHelper.replaceStandardJobIds(job.stream()
                .map(CompactionJobStatus::getJobId)
                .collect(Collectors.toList()), example);
    }

    public static String replaceBracketedJobIds(List<CompactionJobStatus> job, String example) {
        return StatusReporterTestHelper.replaceBracketedJobIds(job.stream()
                .map(CompactionJobStatus::getJobId)
                .collect(Collectors.toList()), example);
    }

    public String verboseReportString(Function<PrintStream, CompactionJobStatusReporter> getReporter, List<CompactionJobStatus> statusList,
                                      Type queryType) {
        ToStringPrintStream out = new ToStringPrintStream();
        getReporter.apply(out.getPrintStream())
                .report(statusList, queryType);
        return out.toString();
    }
}
