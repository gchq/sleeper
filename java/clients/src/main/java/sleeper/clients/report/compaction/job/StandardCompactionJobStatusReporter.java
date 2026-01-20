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

import sleeper.clients.report.job.AverageRowRateReport;
import sleeper.clients.report.job.StandardJobRunReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableFieldDefinition;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.tracker.compaction.job.query.CompactionJobCommittedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobRun;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatusType;
import sleeper.core.tracker.job.run.AverageRowRate;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.util.DurationStatistics;

import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.clients.report.job.StandardJobRunReporter.formatDurationString;
import static sleeper.clients.report.job.StandardJobRunReporter.printUpdateType;

/**
 * Creates reports in human-readable string format on the status of compaction jobs. Depending on the report query type,
 * this produces either a table, or a detailed report for each job run.
 */
public class StandardCompactionJobStatusReporter implements CompactionJobStatusReporter {

    private final TableField stateField;
    private final TableField createTimeField;
    private final TableField jobIdField;
    private final TableField partitionIdField;
    private final TableField inputFilesCount;
    private final StandardJobRunReporter runReporter;
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
        StandardJobRunReporter.Builder runReporterBuilder = StandardJobRunReporter.withTable(tableFactoryBuilder)
                .addProgressFields();
        commitTimeField = tableFactoryBuilder.addField(commitTimeFieldDef);
        runReporter = runReporterBuilder.addResultsFields().build(out);
        finishedFields = Stream.concat(runReporter.getFinishedFields().stream(), Stream.of(commitTimeFieldDef))
                .collect(Collectors.toList());
        tableFactory = tableFactoryBuilder.build();
    }

    @Override
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

            if (!jobStatusList.isEmpty()) {
                out.println();
                out.println("For more information concerning any failure reasons, please consult the more detailed report.");
            }
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
        out.printf("State: %s%n", jobStatus.getFurthestStatusType());
        out.printf("Creation time: %s%n", jobStatus.getCreateUpdateTime());
        out.printf("Partition ID: %s%n", jobStatus.getPartitionId());
        jobStatus.getRunsLatestFirst().forEach(this::printJobRun);
        out.println("--------------------------");
    }

    private void printJobRun(CompactionJobRun run) {
        runReporter.printProcessJobRunWithUpdatePrinter(run,
                printUpdateType(CompactionJobCommittedStatus.class, committedStatus -> printCommitStatus(run, committedStatus)));
        if (run.getStatusType() == CompactionJobStatusType.IN_PROGRESS) {
            out.println("Not finished");
        } else if (run.getStatusType() == CompactionJobStatusType.UNCOMMITTED) {
            out.println("Not committed");
        }
    }

    private void printCommitStatus(JobRunReport run, CompactionJobCommittedStatus committedStatus) {
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
        AverageRowRateReport.printf("Average compaction rate: %s%n", rowRate(jobs), out);
        out.println("Statistics for delay between finish and commit time:");
        out.println("  " + CompactionJobStatus.computeStatisticsOfDelayBetweenFinishAndCommit(jobs)
                .map(DurationStatistics::toString)
                .orElse("no jobs committed"));
    }

    private static AverageRowRate rowRate(List<CompactionJobStatus> jobs) {
        return AverageRowRate.of(jobs.stream()
                .flatMap(job -> job.getRunsLatestFirst().stream()));
    }

    private void writeJob(CompactionJobStatus job, TableWriter.Builder table) {
        if (job.getRunsLatestFirst().isEmpty()) {
            table.row(row -> {
                row.value(stateField, job.getFurthestStatusType());
                writeJobFields(job, row);
            });
        } else {
            job.getRunsLatestFirst().forEach(run -> table.row(row -> {
                writeJobFields(job, row);
                row.value(stateField, run.getStatusType());
                row.value(commitTimeField, run.getCommittedStatus()
                        .map(CompactionJobCommittedStatus::getCommitTime)
                        .orElse(null));
                runReporter.writeRunFields(run, row);
                if (run.getStatusType().equals(CompactionJobStatusType.FAILED)) {
                    row.value(StandardJobRunReporter.FAILURE_REASONS, run.getFailureReasons());
                }
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
