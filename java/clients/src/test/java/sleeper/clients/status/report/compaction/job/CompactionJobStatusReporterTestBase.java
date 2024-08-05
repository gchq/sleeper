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

import sleeper.clients.status.report.StatusReporterTestHelper;
import sleeper.clients.status.report.job.query.JobQuery.Type;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobCommittedStatus;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.status.ProcessRun;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.clients.status.report.StatusReporterTestHelper.task;
import static sleeper.clients.testutil.ClientTestUtils.exampleUUID;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
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

        CompactionJob job1 = dataHelper.singleFileCompaction(partition("A"));
        Instant creationTime1 = Instant.parse("2022-09-17T13:33:12.001Z");
        CompactionJob job2 = dataHelper.singleFileCompaction(partition("B"));
        Instant creationTime2 = Instant.parse("2022-09-18T13:33:12.001Z");
        Instant startedTime2 = Instant.parse("2022-09-18T13:34:12.001Z");
        CompactionJob job3 = dataHelper.singleFileCompaction(partition("C"));
        Instant creationTime3 = Instant.parse("2022-09-19T13:33:12.001Z");
        Instant startedTime3 = Instant.parse("2022-09-19T13:34:12.001Z");
        CompactionJob job4 = dataHelper.singleFileCompaction(partition("D"));
        Instant creationTime4 = Instant.parse("2022-09-20T13:33:12.001Z");
        CompactionJobStartedStatus started4 = compactionStartedStatus(Instant.parse("2022-09-20T13:34:12.001Z"));
        CompactionJobFinishedStatus finished4 = compactionFinishedStatus(summary(started4, Duration.ofMinutes(1), 600, 300));
        CompactionJob job5 = dataHelper.singleFileCompaction(partition("E"));
        Instant creationTime5 = Instant.parse("2022-09-21T13:33:12.001Z");
        CompactionJobStartedStatus started5 = compactionStartedStatus(Instant.parse("2022-09-21T13:34:12.001Z"));
        CompactionJobFinishedStatus finished5 = compactionFinishedStatus(summary(started5, Duration.ofMinutes(1), 600, 300));
        CompactionJobCommittedStatus committed5 = compactionCommittedStatus(Instant.parse("2022-09-21T13:36:12.001Z"));
        CompactionJob job6 = dataHelper.singleFileCompaction(partition("F"));
        Instant creationTime6 = Instant.parse("2022-09-22T13:33:12.001Z");
        CompactionJobStartedStatus started6 = compactionStartedStatus(Instant.parse("2022-09-22T13:34:12.001Z"));
        CompactionJobCommittedStatus committed6 = compactionCommittedStatus(Instant.parse("2022-09-22T13:36:12.001Z"));

        CompactionJobStatus status1 = jobCreated(job1, creationTime1);
        CompactionJobStatus status2 = jobCreated(job2, creationTime2,
                startedCompactionRun(task(1), startedTime2));
        CompactionJobStatus status3 = jobCreated(job3, creationTime3,
                failedCompactionRun(task(1),
                        new ProcessRunTime(startedTime3, Duration.ofMinutes(1)),
                        List.of("Something went wrong", "More details")));
        CompactionJobStatus status4 = jobCreated(job4, creationTime4,
                ProcessRun.builder().taskId(task(1))
                        .startedStatus(started4).finishedStatus(finished4)
                        .build());
        CompactionJobStatus status5 = jobCreated(job5, creationTime5,
                ProcessRun.builder().taskId(task(1))
                        .startedStatus(started5).finishedStatus(finished5).statusUpdate(committed5)
                        .build());
        CompactionJobStatus status6 = jobCreated(job6, creationTime6,
                ProcessRun.builder().taskId(task(1))
                        .startedStatus(started6).statusUpdate(committed6)
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

        CompactionJobStatus succeededThenFailed = jobCreated(dataHelper.singleFileCompaction(),
                Instant.parse("2022-10-10T10:00:00.001Z"),
                failedCompactionRun(task(2), new ProcessRunTime(
                        Instant.parse("2022-10-10T10:01:15.001Z"), Duration.ofSeconds(30)),
                        List.of("Compaction has already been committed", "Failed database update")),
                finishedCompactionRun(task(1), summary(
                        Instant.parse("2022-10-10T10:01:00.001Z"), Duration.ofSeconds(20), 200L, 100L),
                        Instant.parse("2022-10-10T10:01:30.001Z")));

        CompactionJobStatus failedThenInProgress = jobCreated(dataHelper.singleFileCompaction(),
                Instant.parse("2022-10-11T10:00:00.001Z"),
                startedCompactionRun(task(1), Instant.parse("2022-10-11T10:02:00.001Z")),
                failedCompactionRun(task(2), new ProcessRunTime(
                        Instant.parse("2022-10-11T10:01:00.001Z"), Duration.ofSeconds(30)),
                        List.of("Unexpected failure reading input file", "Some temporary IO problem")));

        CompactionJobStatus twoFinishedRunsOneInProgress = jobCreated(dataHelper.singleFileCompaction(),
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
        dataHelper.partitionTree(builder -> builder
                .leavesWithSplits(Arrays.asList(partition("A"), partition("B")), Collections.singletonList("ggg"))
                .parentJoining(partition("C"), partition("A"), partition("B")));

        CompactionJob job1 = dataHelper.singleFileCompaction(partition("C"));
        Instant creationTime1 = Instant.parse("2022-10-13T12:00:00.000Z");
        Instant startedTime1 = Instant.parse("2022-10-13T12:00:10.000Z");
        Instant committedTime1 = Instant.parse("2022-10-13T12:00:30.000Z");
        CompactionJob job2 = dataHelper.singleFileCompaction(partition("C"));
        Instant creationTime2 = Instant.parse("2022-10-13T12:01:00.000Z");
        Instant startedTime2 = Instant.parse("2022-10-13T12:01:10.000Z");
        Instant committedTime2 = Instant.parse("2022-10-13T14:01:30.000Z");

        return Arrays.asList(
                jobCreated(job2, creationTime2, finishedCompactionRun("task-id",
                        summary(startedTime2, Duration.ofHours(2), 1000600, 500300),
                        committedTime2)),
                jobCreated(job1, creationTime1, finishedCompactionRun("task-id",
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
                .jobRunsLatestFirst(List.of())
                .build());
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

    public String verboseReportString(
            Function<PrintStream, CompactionJobStatusReporter> getReporter,
            List<CompactionJobStatus> statusList, Type queryType) {
        ToStringConsoleOutput out = new ToStringConsoleOutput();
        getReporter.apply(out.getPrintStream())
                .report(statusList, queryType);
        return out.toString();
    }
}
