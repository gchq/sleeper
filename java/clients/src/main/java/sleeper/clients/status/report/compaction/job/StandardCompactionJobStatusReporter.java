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

import sleeper.clients.status.report.job.AverageRecordRateReport;
import sleeper.clients.status.report.job.DelayStatistics;
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableFieldDefinition;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriter;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.compaction.job.status.CompactionJobCommittedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusType;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.record.process.status.ProcessRun;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.clients.status.report.job.StandardProcessRunReporter.formatDurationString;
import static sleeper.clients.status.report.job.StandardProcessRunReporter.printUpdateType;

public class StandardCompactionJobStatusReporter implements CompactionJobStatusReporter {

    private final TableField stateField;
    private final TableField createTimeField;
    private final TableField jobIdField;
    private final TableField partitionIdField;
    private final TableField inputFilesCount;
    private final StandardProcessRunReporter runReporter;
    private final TableField commitTimeField;
    private final List<TableFieldDefinition> finishedFields;
    private final TableWriterFactory tableFactory;
    private final PrintStream out;

    public StandardCompactionJobStatusReporter() {
        this(System.out);
    }

    public StandardCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
        TableFieldDefinition commitTimeFieldDef = TableFieldDefinition.field("COMMIT_TIME");
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        createTimeField = tableFactoryBuilder.addField("CREATE_TIME");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        inputFilesCount = tableFactoryBuilder.addNumericField("INPUT_FILES");
        partitionIdField = tableFactoryBuilder.addField("PARTITION_ID");
        StandardProcessRunReporter.Builder runReporterBuilder = StandardProcessRunReporter.withTable(tableFactoryBuilder)
                .addProgressFields();
        commitTimeField = tableFactoryBuilder.addField(commitTimeFieldDef);
        runReporter = runReporterBuilder.addResultsFields().build(out);
        finishedFields = Stream.concat(runReporter.getFinishedFields().stream(), Stream.of(commitTimeFieldDef))
                .collect(Collectors.toList());
        tableFactory = tableFactoryBuilder.build();
    }

    public void report(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType) {
        out.println();
        out.println("Compaction Job Status Report");
        out.println("----------------------------");
        printSummary(jobStatusList, queryType);
        if (!queryType.equals(JobQuery.Type.DETAILED)) {
            tableFactory.tableBuilder()
                    .showFields(queryType != JobQuery.Type.UNFINISHED, finishedFields)
                    .itemsAndSplittingWriter(jobStatusList, this::writeJob)
                    .build().write(out);
        }
    }

    private void printSummary(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType) {
        if (queryType == JobQuery.Type.RANGE) {
            printRangeSummary(jobStatusList);
        } else if (queryType == JobQuery.Type.DETAILED) {
            printDetailedSummary(jobStatusList);
        } else if (queryType == JobQuery.Type.UNFINISHED) {
            printUnfinishedSummary(jobStatusList);
        } else if (queryType == JobQuery.Type.ALL) {
            printAllSummary(jobStatusList);
        }
    }

    private void printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs in defined range: %d%n",
                jobStatusList.size());
        printStatusCounts(jobStatusList);
        printRateAndDelayStatistics(jobStatusList);
    }

    private void printDetailedSummary(List<CompactionJobStatus> jobStatusList) {
        if (jobStatusList.isEmpty()) {
            out.printf("No job found with provided jobId%n");
            out.printf("--------------------------%n");
        } else {
            jobStatusList.forEach(this::printSingleJobSummary);
        }
    }

    private void printSingleJobSummary(CompactionJobStatus jobStatus) {
        out.printf("Details for job %s:%n", jobStatus.getJobId());
        out.printf("State: %s%n", jobStatus.getFurthestRunStatusType());
        out.printf("Creation time: %s%n", jobStatus.getCreateUpdateTime());
        out.printf("Partition ID: %s%n", jobStatus.getPartitionId());
        jobStatus.getJobRuns().forEach(this::printJobRun);
        out.println("--------------------------");
    }

    private void printJobRun(ProcessRun run) {
        runReporter.printProcessJobRunWithUpdatePrinter(run,
                printUpdateType(CompactionJobCommittedStatus.class, committedStatus -> printCommitStatus(run, committedStatus)));
        CompactionJobStatusType runStatusType = CompactionJobStatusType.statusTypeOfJobRun(run);
        if (runStatusType == CompactionJobStatusType.IN_PROGRESS) {
            out.println("Not finished");
        } else if (runStatusType == CompactionJobStatusType.UNCOMMITTED) {
            out.println("Not committed");
        }
    }

    private void printCommitStatus(ProcessRun run, CompactionJobCommittedStatus committedStatus) {
        out.printf("State store commit time: %s%n", committedStatus.getCommitTime());
        if (run.isFinished()) {
            Duration delay = Duration.between(run.getFinishTime(), committedStatus.getCommitTime());
            out.printf("Delay between finish and commit: %s%n", formatDurationString(delay));
        }
    }

    private void printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total unfinished jobs: %d%n", jobStatusList.size());
        printUnfinishedStatusCounts(jobStatusList);
    }

    private void printAllSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs: %d%n", jobStatusList.size());
        printStatusCounts(jobStatusList);
        printRateAndDelayStatistics(jobStatusList);
    }

    private void printStatusCounts(List<CompactionJobStatus> jobStatusList) {
        printUnfinishedStatusCounts(jobStatusList);
        out.printf("Jobs finished successfully: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAnyRunSuccessful).count());
        out.printf("Jobs finished successfully with more than one run: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isMultipleRunsAndAnySuccessful).count());
    }

    private void printUnfinishedStatusCounts(List<CompactionJobStatus> jobStatusList) {
        out.printf("Jobs not yet started: %d%n", jobStatusList.stream().filter(job -> !job.isStarted()).count());
        out.printf("Jobs in an unfinished run: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAnyRunUnfinished).count());
        out.printf("Jobs in an unfinished run after a failed run: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAnyRunUnfinishedAndARunFailed).count());
        out.printf("Failed jobs (may be retried): %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAwaitingRetry).count());
        out.printf("Runs in progress: %d%n", jobStatusList.stream().mapToInt(CompactionJobStatus::getRunsInProgress).sum());
        out.printf("Runs awaiting commit: %d%n", jobStatusList.stream().mapToInt(CompactionJobStatus::getRunsAwaitingCommit).sum());
    }

    private void printRateAndDelayStatistics(List<CompactionJobStatus> jobs) {
        AverageRecordRateReport.printf("Average compaction rate: %s%n", recordRate(jobs), out);
        if (jobs.stream().anyMatch(CompactionJobStatus::isAnyRunSuccessful)) {
            out.println("Statistics for delays between all finish and commit times:");
            out.println("  " + delayStatistics(jobs));
        }
    }

    private static AverageRecordRate recordRate(List<CompactionJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream()));
    }

    private static DelayStatistics delayStatistics(List<CompactionJobStatus> jobs) {
        DelayStatistics.Builder builder = DelayStatistics.builder();
        jobs.stream()
                .flatMap(job -> job.getJobRuns().stream())
                .filter(ProcessRun::isFinishedSuccessfully)
                .forEach(jobRun -> {
                    jobRun.getLastStatusOfType(CompactionJobCommittedStatus.class).ifPresent(commitStatus -> {
                        Instant finishTime = jobRun.getFinishedStatus().getSummary().getFinishTime();
                        Instant commitTime = commitStatus.getCommitTime();
                        builder.add(Duration.between(finishTime, commitTime));
                    });
                });
        return builder.build();
    }

    private void writeJob(CompactionJobStatus job, TableWriter.Builder table) {
        if (job.getJobRuns().isEmpty()) {
            table.row(row -> {
                row.value(stateField, CompactionJobStatusType.PENDING);
                writeJobFields(job, row);
            });
        } else {
            job.getJobRuns().forEach(run -> table.row(row -> {
                writeJobFields(job, row);
                row.value(stateField, CompactionJobStatusType.statusTypeOfJobRun(run));
                row.value(commitTimeField, run.getLastStatusOfType(CompactionJobCommittedStatus.class)
                        .map(CompactionJobCommittedStatus::getCommitTime)
                        .orElse(null));
                runReporter.writeRunFields(run, row);
            }));
        }
    }

    private void writeJobFields(CompactionJobStatus job, TableRow.Builder builder) {
        builder.value(createTimeField, job.getCreateUpdateTime())
                .value(jobIdField, job.getJobId())
                .value(inputFilesCount, job.getInputFilesCount())
                .value(partitionIdField, job.getPartitionId());
    }
}
