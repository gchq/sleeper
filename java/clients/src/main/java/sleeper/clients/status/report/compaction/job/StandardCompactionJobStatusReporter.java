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
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriter;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusType;
import sleeper.core.record.process.AverageRecordRate;

import java.io.PrintStream;
import java.util.List;

import static java.util.function.Predicate.not;

public class StandardCompactionJobStatusReporter implements CompactionJobStatusReporter {

    private final TableField stateField;
    private final TableField createTimeField;
    private final TableField jobIdField;
    private final TableField partitionIdField;
    private final TableField inputFilesCount;
    private final StandardProcessRunReporter runReporter;
    private final TableWriterFactory tableFactory;
    private final PrintStream out;

    public StandardCompactionJobStatusReporter() {
        this(System.out);
    }

    public StandardCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        createTimeField = tableFactoryBuilder.addField("CREATE_TIME");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        inputFilesCount = tableFactoryBuilder.addNumericField("INPUT_FILES");
        partitionIdField = tableFactoryBuilder.addField("PARTITION_ID");
        runReporter = new StandardProcessRunReporter(out, tableFactoryBuilder);
        tableFactory = tableFactoryBuilder.build();
    }

    public void report(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType) {
        out.println();
        out.println("Compaction Job Status Report");
        out.println("----------------------------");
        printSummary(jobStatusList, queryType);
        if (!queryType.equals(JobQuery.Type.DETAILED)) {
            tableFactory.tableBuilder()
                    .showFields(queryType != JobQuery.Type.UNFINISHED, runReporter.getFinishedFields())
                    .itemsAndSplittingWriter(jobStatusList, this::writeJob)
                    .build().write(out);
        }
    }

    private void printSummary(List<CompactionJobStatus> jobStatusList, JobQuery.Type queryType) {
        if (queryType.equals(JobQuery.Type.RANGE)) {
            printRangeSummary(jobStatusList);
        }
        if (queryType.equals(JobQuery.Type.DETAILED)) {
            printDetailedSummary(jobStatusList);
        }
        if (queryType.equals(JobQuery.Type.UNFINISHED)) {
            printUnfinishedSummary(jobStatusList);
        }
        if (queryType.equals(JobQuery.Type.ALL)) {
            printAllSummary(jobStatusList);
        }
    }

    private void printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs in defined range: %d%n",
                jobStatusList.size());
        AverageRecordRateReport.printf("Average compaction rate: %s%n", recordRate(jobStatusList), out);
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
        out.printf("Creation time: %s%n", jobStatus.getCreateUpdateTime().toString());
        out.printf("Partition ID: %s%n", jobStatus.getPartitionId());
        jobStatus.getJobRuns().forEach(runReporter::printProcessJobRun);
        out.println("--------------------------");
    }

    private void printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total unfinished jobs: %d%n", jobStatusList.size());
        out.printf("Total unfinished jobs not started: %d%n",
                jobStatusList.stream().filter(not(CompactionJobStatus::isStarted)).count());
        out.printf("Total unfinished jobs in progress: %d%n",
                jobStatusList.stream().filter(CompactionJobStatus::isAnyRunInProgress).count());
        out.printf("Total unfinished jobs awaiting retry: %d%n",
                jobStatusList.stream().filter(CompactionJobStatus::isAwaitingRetry).count());
    }

    private void printAllSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs: %d%n", jobStatusList.size());
        out.printf("Total jobs not yet started: %d%n", jobStatusList.stream().filter(job -> !job.isStarted()).count());
        out.printf("Total jobs with a run in progress: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAnyRunInProgress).count());
        out.printf("Total jobs finished successfully: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAnyRunSuccessful).count());
        out.printf("Total jobs with any failed run: %d%n", jobStatusList.stream().filter(CompactionJobStatus::isAnyRunFailed).count());
        AverageRecordRateReport.printf("Average compaction rate: %s%n", recordRate(jobStatusList), out);
    }

    private static AverageRecordRate recordRate(List<CompactionJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream()));
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
