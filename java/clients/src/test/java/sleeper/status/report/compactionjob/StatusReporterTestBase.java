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

import sleeper.ToStringPrintStream;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.ProcessRun;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.status.ProcessFinishedStatus;
import sleeper.core.status.ProcessStartedStatus;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.ClientTestUtils.exampleUUID;
import static sleeper.compaction.job.CompactionJobTestDataHelper.finishedCompactionStatus;

public abstract class StatusReporterTestBase {

    protected static String job(int number) {
        return exampleUUID("job", number);
    }

    protected static String partition(String letter) {
        return exampleUUID("partn", letter);
    }

    protected static String task(int number) {
        return exampleUUID("task", number);
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
        return Arrays.asList(status1, status2, status3, status4, status5, status6);
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
                        jobRunStartedInTask(1, "2022-10-12T10:02:00"),
                        jobRunFinishedInTask(2, "2022-10-12T10:01:15", "2022-10-12T10:01:45"),
                        jobRunFinishedInTask(1, "2022-10-12T10:01:00", "2022-10-12T10:01:20")))
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
                        dataHelper.singleFileCompaction(partition("C")),
                        Instant.parse("2022-10-13T12:00:00.000Z"),
                        Duration.ofMillis(123), 600, 300),
                finishedCompactionStatus(
                        dataHelper.singleFileCompaction(partition("C")),
                        Instant.parse("2022-10-13T12:01:00.000Z"),
                        Duration.ofHours(2), 1000600, 500300),
                finishedCompactionStatus(
                        dataHelper.singleFileSplittingCompaction(partition("C"), partition("A"), partition("B")),
                        Instant.parse("2022-10-13T14:01:00.000Z"),
                        Duration.ofSeconds(60), 1000600, 500300),
                finishedCompactionStatus(
                        dataHelper.singleFileSplittingCompaction(partition("C"), partition("A"), partition("B")),
                        Instant.parse("2022-10-13T14:02:00.000Z"),
                        Duration.ofMillis(123), 1234, 1234));
    }

    private static ProcessRun jobRunStartedInTask(int taskNumber, String startTimeNoMillis) {
        return ProcessRun.started(task(taskNumber), defaultStartedStatus(startTimeNoMillis));
    }

    private static ProcessRun jobRunFinishedInTask(int taskNumber, String startTimeNoMillis, String finishTimeNoMillis) {
        return ProcessRun.finished(task(taskNumber),
                defaultStartedStatus(startTimeNoMillis),
                defaultFinishedStatus(startTimeNoMillis, finishTimeNoMillis));
    }

    private static ProcessStartedStatus defaultStartedStatus(String startTimeNoMillis) {
        return ProcessStartedStatus.updateAndStartTime(
                Instant.parse(startTimeNoMillis + ".123Z"),
                Instant.parse(startTimeNoMillis + ".001Z"));
    }

    private static ProcessFinishedStatus defaultFinishedStatus(String startTimeNoMillis, String finishTimeNoMillis) {
        return ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse(finishTimeNoMillis + ".123Z"),
                new RecordsProcessedSummary(
                        new RecordsProcessed(300L, 200L),
                        Instant.parse(startTimeNoMillis + ".001Z"),
                        Instant.parse(finishTimeNoMillis + ".001Z")));
    }

    protected static String replaceStandardJobIds(List<CompactionJobStatus> jobs, String example) {
        return replaceJobIds(jobs, StatusReporterTestBase::job, example);
    }

    protected static String replaceBracketedJobIds(List<CompactionJobStatus> jobs, String example) {
        return replaceJobIds(jobs, number -> "$(jobId" + number + ")", example);
    }

    protected static String replaceJobIds(
            List<CompactionJobStatus> jobs, Function<Integer, String> getTemplateId, String example) {
        String replaced = example;
        for (int i = 0; i < jobs.size(); i++) {
            replaced = replaced.replace(getTemplateId.apply(i + 1), jobs.get(i).getJobId());
        }
        return replaced;
    }

    protected static CompactionJobStatus jobCreated(CompactionJob job, Instant creationTime) {
        return CompactionJobStatus.created(job, creationTime);
    }

    protected static CompactionJobStatus jobStarted(CompactionJob job, String taskId, Instant creationTime, Instant startTime, Instant startUpdateTime) {
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, creationTime))
                .singleJobRun(ProcessRun.started(taskId, ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime)))
                .build();
    }

    protected static CompactionJobStatus jobFinished(CompactionJob job, String taskId, Instant creationTime, Instant startTime, Instant startUpdateTime, Instant finishedTime) {
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(600L, 300L), startUpdateTime, finishedTime);
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, creationTime))
                .singleJobRun(ProcessRun.finished(taskId, ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime),
                        ProcessFinishedStatus.updateTimeAndSummary(finishedTime, summary)))
                .build();
    }

    public String verboseReportString(Function<PrintStream, CompactionJobStatusReporter> getReporter, List<CompactionJobStatus> statusList,
                                      QueryType queryType) {
        ToStringPrintStream out = new ToStringPrintStream();
        getReporter.apply(out.getPrintStream())
                .report(statusList, queryType);
        return out.toString();
    }
}
