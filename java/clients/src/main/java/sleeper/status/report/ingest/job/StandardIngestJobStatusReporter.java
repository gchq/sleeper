/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.status.report.job.AverageRecordRateReport;
import sleeper.status.report.job.StandardProcessRunReporter;
import sleeper.status.report.job.query.JobQuery;
import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableRow;
import sleeper.status.report.table.TableWriter;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;
import java.util.List;

import static sleeper.status.report.job.StandardProcessRunReporter.STATE_FINISHED;
import static sleeper.status.report.job.StandardProcessRunReporter.STATE_IN_PROGRESS;

public class StandardIngestJobStatusReporter implements IngestJobStatusReporter {

    private final TableField stateField;
    private final TableField jobIdField;
    private final TableField totalFilesField;
    private final TableWriterFactory tableFactory;
    private final StandardProcessRunReporter runReporter;

    private final PrintStream out;

    public StandardIngestJobStatusReporter() {
        this(System.out);
    }

    public StandardIngestJobStatusReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        totalFilesField = tableFactoryBuilder.addNumericField("TOTAL_FILES");
        runReporter = new StandardProcessRunReporter(out, tableFactoryBuilder);
        tableFactory = tableFactoryBuilder.build();
    }

    public void report(List<IngestJobStatus> statusList, JobQuery.Type query, int numberInQueue) {
        out.println();
        out.println("Ingest Job Status Report");
        out.println("------------------------");
        printSummary(statusList, query, numberInQueue);
        if (!query.equals(JobQuery.Type.DETAILED)) {
            tableFactory.tableBuilder()
                    .showFields(query != JobQuery.Type.UNFINISHED, runReporter.getFinishedFields())
                    .itemsAndSplittingWriter(statusList, this::writeJob)
                    .build().write(out);
        }
    }

    private void printSummary(List<IngestJobStatus> statusList, JobQuery.Type queryType, int numberInQueue) {
        if (queryType.equals(JobQuery.Type.DETAILED)) {
            printDetailedSummary(statusList);
        } else if (queryType.equals(JobQuery.Type.ALL)) {
            printAllSummary(statusList, numberInQueue);
        } else if (queryType.equals(JobQuery.Type.UNFINISHED)) {
            printUnfinishedSummary(statusList, numberInQueue);
        }
    }

    private void printDetailedSummary(List<IngestJobStatus> statusList) {
        if (statusList.isEmpty()) {
            out.println("No job found with provided jobId");
            out.println("------------------------");
        } else {
            for (IngestJobStatus status : statusList) {
                if (status == null) {
                    out.println("No job found with provided jobId");
                } else {
                    printDetailedSummary(status);
                }
                out.println("------------------------");
            }
        }
    }

    private void printDetailedSummary(IngestJobStatus status) {
        out.printf("Details for job %s:%n", status.getJobId());
        out.printf("State: %s%n", status.isFinished() ? STATE_FINISHED : STATE_IN_PROGRESS);
        out.printf("Number of input files: %d%n", status.getInputFilesCount());
        for (ProcessRun run : status.getJobRuns()) {
            runReporter.printProcessJobRun(run);
        }
    }

    private void printAllSummary(List<IngestJobStatus> statusList, int numberInQueue) {
        printUnfinishedSummary(statusList, numberInQueue);
        out.printf("Total jobs finished: %s%n", statusList.stream().filter(IngestJobStatus::isFinished).count());
        AverageRecordRateReport.printf("Average ingest rate: %s%n", recordRate(statusList), out);
    }

    private void printUnfinishedSummary(List<IngestJobStatus> statusList, int numberInQueue) {
        out.printf("Total jobs waiting in queue (excluded from report): %s%n", numberInQueue);
        out.printf("Total jobs in progress: %s%n", statusList.stream().filter(status -> !status.isFinished()).count());
    }

    private static AverageRecordRate recordRate(List<IngestJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream()));
    }

    private void writeJob(IngestJobStatus job, TableWriter.Builder table) {
        job.getJobRuns().forEach(run -> table.row(row -> {
            writeJobFields(job, row);
            row.value(stateField, StandardProcessRunReporter.getState(run));
            runReporter.writeRunFields(run, row);
        }));

    }

    private void writeJobFields(IngestJobStatus job, TableRow.Builder builder) {
        builder.value(jobIdField, job.getJobId())
                .value(totalFilesField, job.getInputFilesCount());
    }
}
