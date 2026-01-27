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

package sleeper.clients.report.ingest.job;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import sleeper.clients.report.job.AverageRowRateReport;
import sleeper.clients.report.job.StandardJobRunReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriter;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRun;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobValidatedStatus;
import sleeper.core.tracker.job.run.AverageRowRate;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static sleeper.clients.report.job.StandardJobRunReporter.printUpdateType;
import static sleeper.clients.report.job.StandardJobRunReporter.updatePrinters;
import static sleeper.core.tracker.ingest.job.query.IngestJobStatusType.IN_PROGRESS;

/**
 * Creates reports in human-readable string format on the status of ingest and bulk import jobs. Depending on the report
 * query type, this produces either a table, or a detailed report for each job run.
 */
public class StandardIngestJobStatusReporter implements IngestJobStatusReporter {

    private final TableField stateField;
    private final TableField jobIdField;
    private final TableField inputFilesCount;
    private final TableField addedFilesCount;
    private final TableWriterFactory tableFactory;
    private final StandardJobRunReporter runReporter;

    private final PrintStream out;

    public StandardIngestJobStatusReporter() {
        this(System.out);
    }

    public StandardIngestJobStatusReporter(PrintStream out) {
        this.out = out;
        TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
        stateField = tableFactoryBuilder.addField("STATE");
        jobIdField = tableFactoryBuilder.addField("JOB_ID");
        inputFilesCount = tableFactoryBuilder.addNumericField("INPUT_FILES");
        addedFilesCount = tableFactoryBuilder.addNumericField("ADDED_FILES");
        runReporter = StandardJobRunReporter.withTable(tableFactoryBuilder)
                .addProgressFields()
                .addResultsFields()
                .build(out);
        tableFactory = tableFactoryBuilder.build();
    }

    @Override
    public void report(
            List<IngestJobStatus> statusList, JobQuery.Type query, IngestQueueMessages queueMessages,
            Map<String, Integer> persistentEmrStepCount) {
        out.println();
        out.println("Ingest Job Status Report");
        out.println("------------------------");
        printSummary(statusList, query, queueMessages, persistentEmrStepCount);
        if (!query.equals(JobQuery.Type.DETAILED)) {
            tableFactory.tableBuilder()
                    .showFields(query != JobQuery.Type.UNFINISHED && query != JobQuery.Type.REJECTED,
                            runReporter.getFinishedFields())
                    .showField(query != JobQuery.Type.REJECTED, addedFilesCount)
                    .itemsAndSplittingWriter(statusList, this::writeJob)
                    .build().write(out);

            if (!statusList.isEmpty()) {
                out.println();
                out.println("For more information concerning any failure reasons, please consult the more detailed report.");
            }
        }
    }

    private void printSummary(
            List<IngestJobStatus> statusList, JobQuery.Type queryType,
            IngestQueueMessages queueMessages, Map<String, Integer> persistentEmrStepCount) {
        if (queryType.equals(JobQuery.Type.DETAILED)) {
            printDetailedSummary(statusList);
        } else if (queryType.equals(JobQuery.Type.ALL)) {
            printAllSummary(statusList, queueMessages, persistentEmrStepCount);
        } else if (queryType.equals(JobQuery.Type.UNFINISHED)) {
            printUnfinishedSummary(statusList, queueMessages, persistentEmrStepCount);
        } else if (queryType.equals(JobQuery.Type.RANGE)) {
            printRangeSummary(statusList, queueMessages);
        } else if (queryType.equals(JobQuery.Type.REJECTED)) {
            printRejectedSummary(statusList, queueMessages);
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
        out.printf("State: %s%n", status.getFurthestRunStatusType());
        out.printf("Number of input files: %d%n", status.getInputFileCount());
        for (IngestJobRun run : status.getRunsLatestFirst()) {
            printProcessJobRun(run);
        }
    }

    private void printProcessJobRun(IngestJobRun run) {
        runReporter.printProcessJobRunWithUpdatePrinter(run, updatePrinters(
                printUpdateType(IngestJobValidatedStatus.class, this::printValidation),
                printUpdateType(IngestJobAddedFilesStatus.class, this::printAddedFiles)));
        if (run.getStatusType() == IN_PROGRESS) {
            out.println("Not finished");
        }
    }

    private void printValidation(IngestJobValidatedStatus update) {
        out.printf("Validation time: %s%n", update.getStartTime());
        out.printf("Validation update time: %s%n", update.getUpdateTime());
        if (update.isValid()) {
            out.println("Job was accepted");
        } else {
            out.println("Job was rejected with reasons:");
            IngestJobRejectedStatus rejectedStatus = (IngestJobRejectedStatus) update;
            rejectedStatus.getFailureReasons().forEach(reason -> out.printf("- %s%n", reason));
            if (rejectedStatus.getJsonMessage() != null) {
                out.println();
                out.println("Received JSON message:");
                out.println(prettyPrintJsonString(rejectedStatus.getJsonMessage()));
                out.println();
            }
        }
    }

    private void printAddedFiles(IngestJobAddedFilesStatus update) {
        out.printf("%s files written at: %s%n", update.getFileCount(), update.getWrittenTime());
        out.printf("Files added to table at: %s%n", update.getUpdateTime());
    }

    private String prettyPrintJsonString(String json) {
        Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();
        try {
            return prettyGson.toJson(prettyGson.fromJson(json, JsonObject.class));
        } catch (RuntimeException e) {
            return json;
        }
    }

    private void printAllSummary(
            List<IngestJobStatus> statusList, IngestQueueMessages queueMessages,
            Map<String, Integer> persistentEmrStepCount) {
        printUnfinishedSummary(statusList, queueMessages, persistentEmrStepCount);
        out.printf("Total jobs finished: %s%n", statusList.stream().filter(IngestJobStatus::isAnyRunSuccessful).count());
        AverageRowRateReport.printf("Average ingest rate: %s%n", rowRate(statusList), out);
    }

    private void printUnfinishedSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages,
            Map<String, Integer> persistentEmrStepCount) {
        queueMessages.print(out);
        printPendingEmrStepCount(persistentEmrStepCount);
        out.printf("Total jobs in report: %s%n", statusList.size());
        out.printf("Total jobs in progress: %s%n", statusList.stream().filter(IngestJobStatus::isAnyRunInProgress).count());
    }

    private void printPendingEmrStepCount(Map<String, Integer> stepCount) {
        if (!stepCount.isEmpty()) {
            out.printf("Total persistent EMR steps pending: %s%n", stepCount.getOrDefault("PENDING", 0));
        }
    }

    private void printRangeSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages) {
        queueMessages.print(out);
        out.printf("Total jobs in defined range: %d%n", statusList.size());
        AverageRowRateReport.printf("Average ingest rate: %s%n", rowRate(statusList), out);
    }

    private void printRejectedSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages) {
        queueMessages.print(out);
        out.printf("Total jobs rejected: %d%n", statusList.size());
    }

    private static AverageRowRate rowRate(List<IngestJobStatus> jobs) {
        return AverageRowRate.of(jobs.stream()
                .flatMap(job -> job.getRunsLatestFirst().stream()));
    }

    private void writeJob(IngestJobStatus job, TableWriter.Builder table) {
        job.getRunsLatestFirst().forEach(run -> table.row(row -> {
            writeJobFields(job, row);
            row.value(stateField, run.getStatusType());
            row.value(addedFilesCount, run.getFilesWrittenAndAdded().getFilesAddedToStateStore());
            runReporter.writeRunFields(run, row);
            row.value(StandardJobRunReporter.FAILURE_REASONS, run.getFailureReasonsDisplay(30));
        }));
    }

    private void writeJobFields(IngestJobStatus job, TableRow.Builder builder) {
        builder.value(jobIdField, job.getJobId())
                .value(inputFilesCount, job.getInputFileCount());
    }
}
