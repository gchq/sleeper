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

import sleeper.clients.report.job.query.JobQuery.Type;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobTestDataHelper;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.JobRuns;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.clients.report.StatusReporterTestHelper.job;
import static sleeper.clients.report.StatusReporterTestHelper.task;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.testutils.SupplierTestHelper.exampleUUID;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJobRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

public abstract class CompactionJobStatusReporterTestBase {

    protected static String partition(String letter) {
        return exampleUUID("partn", letter);
    }

    protected static List<CompactionJobStatus> mixedJobStatuses() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        dataHelper.partitionTreeFromSchema(schema -> PartitionsBuilderSplitsFirst
                .leavesWithSplits(schema,
                        List.of(partition("A"), partition("B"), partition("C"), partition("D")),
                        List.of("ddd", "ggg", "kkk"))
                .parentJoining(partition("E"), partition("A"), partition("B"))
                .parentJoining(partition("F"), partition("E"), partition("C"))
                .parentJoining(partition("G"), partition("F"), partition("D")));

        CompactionJobStatus status1 = compactionJobCreated(
                dataHelper.singleFileCompaction(job(1), partition("A")),
                Instant.parse("2022-09-17T13:33:12.001Z"));
        CompactionJobStatus status2 = compactionJobCreated(
                dataHelper.singleFileCompaction(job(2), partition("B")),
                Instant.parse("2022-09-18T13:33:12.001Z"),
                startedCompactionRun(task(1), Instant.parse("2022-09-18T13:34:12.001Z")));
        CompactionJobStatus status3 = compactionJobCreated(
                dataHelper.singleFileCompaction(job(3), partition("C")),
                Instant.parse("2022-09-19T13:33:12.001Z"),
                failedCompactionRun(task(1),
                        new JobRunTime(Instant.parse("2022-09-19T13:34:12.001Z"), Duration.ofMinutes(1)),
                        List.of("Something went wrong", "More details")));
        CompactionJobStatus status4 = compactionJobCreated(
                dataHelper.singleFileCompaction(job(4), partition("D")),
                Instant.parse("2022-09-20T13:33:12.001Z"),
                jobRunOnTask(task(1),
                        compactionStartedStatus(Instant.parse("2022-09-20T13:34:12.001Z")),
                        compactionFinishedStatus(summary(Instant.parse("2022-09-20T13:34:12.001Z"), Duration.ofMinutes(1), 600, 300))));
        CompactionJobStatus status5 = compactionJobCreated(
                dataHelper.singleFileCompaction(job(5), partition("E")),
                Instant.parse("2022-09-21T13:33:12.001Z"),
                jobRunOnTask(task(1),
                        compactionStartedStatus(Instant.parse("2022-09-21T13:34:12.001Z")),
                        compactionFinishedStatus(summary(Instant.parse("2022-09-21T13:34:12.001Z"), Duration.ofMinutes(1), 600, 300)),
                        compactionCommittedStatus(Instant.parse("2022-09-21T13:36:12.001Z"))));
        CompactionJobStatus status6 = compactionJobCreated(
                dataHelper.singleFileCompaction(job(6), partition("F")),
                Instant.parse("2022-09-22T13:33:12.001Z"),
                jobRunOnTask(task(1),
                        compactionStartedStatus(Instant.parse("2022-09-22T13:34:12.001Z")),
                        compactionCommittedStatus(Instant.parse("2022-09-22T13:36:12.001Z"))));
        return Arrays.asList(status6, status5, status4, status3, status2, status1);
    }

    protected static List<CompactionJobStatus> mixedUnfinishedJobStatuses() {
        return mixedJobStatuses().stream()
                .filter(CompactionJobStatus::isUnstartedOrInProgress)
                .collect(Collectors.toList());
    }

    protected static List<CompactionJobStatus> jobsWithMultipleRuns() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

        CompactionJobStatus succeededThenFailed = compactionJobCreated(dataHelper.singleFileCompaction(job(1)),
                Instant.parse("2022-10-10T10:00:00.001Z"),
                failedCompactionRun(task(2), new JobRunTime(
                        Instant.parse("2022-10-10T10:01:15.001Z"), Duration.ofSeconds(30)),
                        List.of("Compaction has already been committed", "Failed database update")),
                finishedCompactionRun(task(1), summary(
                        Instant.parse("2022-10-10T10:01:00.001Z"), Duration.ofSeconds(20), 200L, 100L),
                        Instant.parse("2022-10-10T10:01:30.001Z")));

        CompactionJobStatus failedThenInProgress = compactionJobCreated(dataHelper.singleFileCompaction(job(2)),
                Instant.parse("2022-10-11T10:00:00.001Z"),
                startedCompactionRun(task(1), Instant.parse("2022-10-11T10:02:00.001Z")),
                failedCompactionRun(task(2), new JobRunTime(
                        Instant.parse("2022-10-11T10:01:00.001Z"), Duration.ofSeconds(30)),
                        List.of("Unexpected failure reading input file", "Some temporary IO problem")));

        CompactionJobStatus twoFinishedRunsOneInProgress = compactionJobCreated(dataHelper.singleFileCompaction(job(3)),
                Instant.parse("2022-10-12T10:00:00.001Z"),
                startedCompactionRun(task(1), Instant.parse("2022-10-12T10:02:00.001Z")),
                finishedCompactionRun(task(2), summary(
                        Instant.parse("2022-10-12T10:01:15.001Z"), Duration.ofSeconds(30), 300L, 150L),
                        Instant.parse("2022-10-12T10:02:00.001Z")),
                finishedCompactionRun(task(1), summary(
                        Instant.parse("2022-10-12T10:01:00.001Z"), Duration.ofSeconds(30), 300L, 150L),
                        Instant.parse("2022-10-12T10:01:45.001Z")));

        return List.of(twoFinishedRunsOneInProgress, failedThenInProgress, succeededThenFailed);
    }

    protected static List<CompactionJobStatus> jobsWithLargeAndDecimalStatistics() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        dataHelper.partitionTreeFromSchema(schema -> PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of(partition("A"), partition("B")), List.of("ggg"))
                .parentJoining(partition("C"), partition("A"), partition("B")));

        CompactionJob job1 = dataHelper.singleFileCompaction(job(1), partition("C"));
        Instant creationTime1 = Instant.parse("2022-10-13T12:00:00.000Z");
        Instant startedTime1 = Instant.parse("2022-10-13T12:00:10.000Z");
        Instant committedTime1 = Instant.parse("2022-10-13T12:00:30.000Z");
        CompactionJob job2 = dataHelper.singleFileCompaction(job(2), partition("C"));
        Instant creationTime2 = Instant.parse("2022-10-13T12:01:00.000Z");
        Instant startedTime2 = Instant.parse("2022-10-13T12:01:10.000Z");
        Instant committedTime2 = Instant.parse("2022-10-13T14:01:30.000Z");

        return Arrays.asList(
                compactionJobCreated(job2, creationTime2, finishedCompactionRun("task-id",
                        summary(startedTime2, Duration.ofHours(2), 1000600, 500300),
                        committedTime2)),
                compactionJobCreated(job1, creationTime1, finishedCompactionRun("task-id",
                        summary(startedTime1, Duration.ofMillis(123), 600, 300),
                        committedTime1)));
    }

    protected static List<CompactionJobStatus> jobWithMultipleInputFiles() {
        Instant creationTime = Instant.parse("2022-10-13T12:00:00.001Z");
        return List.of(CompactionJobStatus.builder().jobId("test-job")
                .createdStatus(CompactionJobCreatedStatus.builder()
                        .inputFilesCount(5)
                        .updateTime(creationTime)
                        .partitionId("test-partition")
                        .build())
                .jobRuns(JobRuns.latestFirst(List.of()))
                .build());
    }

    protected static List<CompactionJobStatus> partialJobStatuses() {
        return CompactionJobStatus.listFrom(records().fromUpdates(
                forJobRunOnTask("job-1",
                        compactionFinishedStatus(
                                Instant.parse("2025-05-14T10:47:00Z"),
                                new RowsProcessed(10, 5)),
                        compactionCommittedStatus(
                                Instant.parse("2025-05-14T10:47:01Z"))))
                .stream());
    }

    protected static String verboseReportString(
            Function<PrintStream, CompactionJobStatusReporter> getReporter,
            List<CompactionJobStatus> statusList, Type queryType) {
        ToStringConsoleOutput out = new ToStringConsoleOutput();
        getReporter.apply(out.getPrintStream())
                .report(statusList, queryType);
        return out.toString();
    }
}
