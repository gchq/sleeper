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

package sleeper.clients.status.report.ingest.job;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import sleeper.clients.status.report.job.AverageRecordRateReport;
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriter;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.tracker.ingest.job.IngestJobFilesWrittenAndAdded;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobStatusType;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobValidatedStatus;
import sleeper.core.tracker.job.run.AverageRecordRate;
import sleeper.core.tracker.job.run.JobRun;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static sleeper.clients.status.report.job.StandardProcessRunReporter.printUpdateType;
import static sleeper.clients.status.report.job.StandardProcessRunReporter.updatePrinters;
import static sleeper.core.tracker.ingest.job.IngestJobStatusType.IN_PROGRESS;

public class StandardIngestJobStatusReporter implements IngestJobStatusReporter {

    private final TableField stateField;
    private final TableField jobIdField;
    private final TableField inputFilesCount;
    private final TableField addedFilesCount;
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
        inputFilesCount = tableFactoryBuilder.addNumericField("INPUT_FILES");
        addedFilesCount = tableFactoryBuilder.addNumericField("ADDED_FILES");
        runReporter = new StandardProcessRunReporter(out, tableFactoryBuilder);
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
        out.printf("Number of input files: %d%n", status.getInputFilesCount());
        for (JobRun run : status.getJobRuns()) {
            printProcessJobRun(run);
        }
    }

    private void printProcessJobRun(JobRun run) {
        runReporter.printProcessJobRunWithUpdatePrinter(run, updatePrinters(
                printUpdateType(IngestJobValidatedStatus.class, this::printValidation),
                printUpdateType(IngestJobAddedFilesStatus.class, this::printAddedFiles)));
        if (IngestJobStatusType.statusTypeOfJobRun(run) == IN_PROGRESS) {
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
        AverageRecordRateReport.printf("Average ingest rate: %s%n", recordRate(statusList), out);
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
        AverageRecordRateReport.printf("Average ingest rate: %s%n", recordRate(statusList), out);
    }

    private void printRejectedSummary(List<IngestJobStatus> statusList, IngestQueueMessages queueMessages) {
        queueMessages.print(out);
        out.printf("Total jobs rejected: %d%n", statusList.size());
    }

    private static AverageRecordRate recordRate(List<IngestJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream()));
    }

    private void writeJob(IngestJobStatus job, TableWriter.Builder table) {
        job.getJobRuns().forEach(run -> table.row(row -> {
            writeJobFields(job, row);
            row.value(stateField, IngestJobStatusType.statusTypeOfJobRun(run));
            row.value(addedFilesCount, IngestJobFilesWrittenAndAdded.from(run).getFilesAddedToStateStore());
            runReporter.writeRunFields(run, row);
        }));
    }

    private void writeJobFields(IngestJobStatus job, TableRow.Builder builder) {
        builder.value(jobIdField, job.getJobId())
                .value(inputFilesCount, job.getInputFilesCount());
    }
}
