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

import sleeper.clients.status.report.job.query.JobQuery.Type;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobTestDataHelper;
import sleeper.compaction.core.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.core.job.status.CompactionJobInputFilesAssignedStatus;
import sleeper.compaction.core.job.status.CompactionJobStatus;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.status.ProcessRun;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.clients.status.report.StatusReporterTestHelper.job;
import static sleeper.clients.status.report.StatusReporterTestHelper.task;
import static sleeper.clients.testutil.ClientTestUtils.exampleUUID;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobFilesAssigned;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;

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

        CompactionJobStatus status1 = jobFilesAssigned(
                dataHelper.singleFileCompaction(job(1), partition("A")),
                Instant.parse("2022-09-17T13:33:12.001Z"),
                Instant.parse("2022-09-17T13:33:12.001Z"));
        CompactionJobStatus status2 = jobFilesAssigned(
                dataHelper.singleFileCompaction(job(2), partition("B")),
                Instant.parse("2022-09-18T13:33:12.001Z"),
                Instant.parse("2022-09-18T13:33:12.001Z"),
                startedCompactionRun(task(1), Instant.parse("2022-09-18T13:34:12.001Z")));
        CompactionJobStatus status3 = jobFilesAssigned(
                dataHelper.singleFileCompaction(job(3), partition("C")),
                Instant.parse("2022-09-19T13:33:12.001Z"),
                Instant.parse("2022-09-19T13:33:12.001Z"),
                failedCompactionRun(task(1),
                        new ProcessRunTime(Instant.parse("2022-09-19T13:34:12.001Z"), Duration.ofMinutes(1)),
                        List.of("Something went wrong", "More details")));
        CompactionJobStatus status4 = jobFilesAssigned(
                dataHelper.singleFileCompaction(job(4), partition("D")),
                Instant.parse("2022-09-20T13:33:12.001Z"),
                Instant.parse("2022-09-20T13:33:12.001Z"),
                ProcessRun.builder().taskId(task(1))
                        .startedStatus(compactionStartedStatus(Instant.parse("2022-09-20T13:34:12.001Z")))
                        .finishedStatus(compactionFinishedStatus(summary(Instant.parse("2022-09-20T13:34:12.001Z"), Duration.ofMinutes(1), 600, 300)))
                        .build());
        CompactionJobStatus status5 = jobFilesAssigned(
                dataHelper.singleFileCompaction(job(5), partition("E")),
                Instant.parse("2022-09-21T13:33:12.001Z"),
                Instant.parse("2022-09-21T13:33:12.001Z"),
                ProcessRun.builder().taskId(task(1))
                        .startedStatus(compactionStartedStatus(Instant.parse("2022-09-21T13:34:12.001Z")))
                        .finishedStatus(compactionFinishedStatus(summary(Instant.parse("2022-09-21T13:34:12.001Z"), Duration.ofMinutes(1), 600, 300)))
                        .statusUpdate(compactionCommittedStatus(Instant.parse("2022-09-21T13:36:12.001Z")))
                        .build());
        CompactionJobStatus status6 = jobFilesAssigned(
                dataHelper.singleFileCompaction(job(6), partition("F")),
                Instant.parse("2022-09-22T13:33:12.001Z"),
                Instant.parse("2022-09-22T13:33:12.001Z"),
                ProcessRun.builder().taskId(task(1))
                        .startedStatus(compactionStartedStatus(Instant.parse("2022-09-22T13:34:12.001Z")))
                        .statusUpdate(compactionCommittedStatus(Instant.parse("2022-09-22T13:36:12.001Z")))
                        .build());
        return Arrays.asList(status6, status5, status4, status3, status2, status1);
    }

    protected static List<CompactionJobStatus> mixedUnfinishedJobStatuses() {
        return mixedJobStatuses().stream()
                .filter(CompactionJobStatus::isUnstartedOrInProgress)
                .collect(Collectors.toList());
    }

    protected static List<CompactionJobStatus> jobsWithMultipleRuns() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

        CompactionJobStatus succeededThenFailed = jobFilesAssigned(dataHelper.singleFileCompaction(job(1)),
                Instant.parse("2022-10-10T10:00:00.001Z"),
                Instant.parse("2022-10-10T10:00:00.001Z"),
                failedCompactionRun(task(2), new ProcessRunTime(
                        Instant.parse("2022-10-10T10:01:15.001Z"), Duration.ofSeconds(30)),
                        List.of("Compaction has already been committed", "Failed database update")),
                finishedCompactionRun(task(1), summary(
                        Instant.parse("2022-10-10T10:01:00.001Z"), Duration.ofSeconds(20), 200L, 100L),
                        Instant.parse("2022-10-10T10:01:30.001Z")));

        CompactionJobStatus failedThenInProgress = jobFilesAssigned(dataHelper.singleFileCompaction(job(2)),
                Instant.parse("2022-10-11T10:00:00.001Z"),
                Instant.parse("2022-10-11T10:00:00.001Z"),
                startedCompactionRun(task(1), Instant.parse("2022-10-11T10:02:00.001Z")),
                failedCompactionRun(task(2), new ProcessRunTime(
                        Instant.parse("2022-10-11T10:01:00.001Z"), Duration.ofSeconds(30)),
                        List.of("Unexpected failure reading input file", "Some temporary IO problem")));

        CompactionJobStatus twoFinishedRunsOneInProgress = jobFilesAssigned(dataHelper.singleFileCompaction(job(3)),
                Instant.parse("2022-10-12T10:00:00.001Z"),
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
                jobFilesAssigned(job2, creationTime2, creationTime2, finishedCompactionRun("task-id",
                        summary(startedTime2, Duration.ofHours(2), 1000600, 500300),
                        committedTime2)),
                jobFilesAssigned(job1, creationTime1, creationTime1, finishedCompactionRun("task-id",
                        summary(startedTime1, Duration.ofMillis(123), 600, 300),
                        committedTime1)));
    }

    protected List<CompactionJobStatus> jobWithMultipleInputFiles() {
        Instant creationTime = Instant.parse("2022-10-13T12:00:00.001Z");
        return List.of(CompactionJobStatus.builder().jobId("test-job")
                .createdStatus(CompactionJobCreatedStatus.builder()
                        .inputFilesCount(5)
                        .updateTime(creationTime)
                        .partitionId("test-partition")
                        .build())
                .filesAssignedStatus(CompactionJobInputFilesAssignedStatus.builder()
                        .inputFilesCount(5)
                        .updateTime(creationTime)
                        .partitionId("test-partition")
                        .build())
                .jobRunsLatestFirst(List.of())
                .build());
    }

    public String verboseReportString(
            Function<PrintStream, CompactionJobStatusReporter> getReporter,
            List<CompactionJobStatus> statusList, Type queryType) {
        ToStringConsoleOutput out = new ToStringConsoleOutput();
        getReporter.apply(out.getPrintStream())
                .report(statusList, queryType);
        return out.toString();
    }
}
