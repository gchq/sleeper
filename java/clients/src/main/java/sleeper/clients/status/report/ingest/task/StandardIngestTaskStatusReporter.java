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
package sleeper.clients.status.report.ingest.task;

import sleeper.clients.status.report.job.AverageRecordRateReport;
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;

import java.io.PrintStream;
import java.util.List;

public class StandardIngestTaskStatusReporter implements IngestTaskStatusReporter {

    private static final TableWriterFactory.Builder TABLE_FACTORY_BUILDER = TableWriterFactory.builder();

    private static final TableField STATE = TABLE_FACTORY_BUILDER.addField("STATE");
    private static final TableField TASK_ID = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.TASK_ID);
    private static final TableField START_TIME = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.START_TIME);
    private static final TableField FINISH_TIME = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.FINISH_TIME);
    private static final TableField DURATION = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.DURATION);
    private static final TableField JOB_RUNS = TABLE_FACTORY_BUILDER.addNumericField("JOB_RUNS");
    private static final TableField JOB_DURATION = TABLE_FACTORY_BUILDER.addNumericField("JOB_DURATION");
    private static final TableField RECORDS_READ = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.RECORDS_READ);
    private static final TableField RECORDS_WRITTEN = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.RECORDS_WRITTEN);
    private static final TableField READ_RATE = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.READ_RATE);
    private static final TableField WRITE_RATE = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.WRITE_RATE);

    private static final TableWriterFactory TABLE_FACTORY = TABLE_FACTORY_BUILDER.build();

    private final PrintStream out;
    private final StandardProcessRunReporter processRunReporter;

    public StandardIngestTaskStatusReporter(PrintStream out) {
        this.out = out;
        processRunReporter = new StandardProcessRunReporter(out);
    }

    @Override
    public void report(IngestTaskQuery query, List<IngestTaskStatus> tasks) {
        out.println();
        out.println("Ingest Task Status Report");
        out.println("-------------------------");
        if (query == IngestTaskQuery.UNFINISHED) {
            printUnfinishedSummary(tasks);
        } else if (query == IngestTaskQuery.ALL) {
            printAllSummary(tasks);
        }

        TABLE_FACTORY.tableBuilder()
                .showFields(query != IngestTaskQuery.UNFINISHED,
                        FINISH_TIME, DURATION, JOB_RUNS, JOB_DURATION, RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE)
                .itemsAndWriter(tasks, this::writeRow)
                .build().write(out);
    }

    private void printUnfinishedSummary(List<IngestTaskStatus> tasks) {
        out.printf("Total tasks in progress: %s%n", tasks.size());
    }

    private void printAllSummary(List<IngestTaskStatus> tasks) {
        out.printf("Total tasks: %s%n", tasks.size());
        out.printf("Total tasks in progress: %s%n", tasks.stream().filter(task -> !task.isFinished()).count());
        out.printf("Total tasks finished: %s%n", tasks.stream().filter(IngestTaskStatus::isFinished).count());
        if (tasks.stream().anyMatch(IngestTaskStatus::isFinished)) {
            out.printf("Total job runs: %s%n", getTotalJobsRun(tasks));
        }
        AverageRecordRateReport.printf("Average standard compaction rate: %s%n", recordRate(tasks), out);
    }

    private static int getTotalJobsRun(List<IngestTaskStatus> tasks) {
        return tasks.stream().mapToInt(IngestTaskStatus::getJobRuns).sum();
    }

    private static AverageRecordRate recordRate(List<IngestTaskStatus> tasks) {
        return AverageRecordRate.of(tasks.stream()
                .map(IngestTaskStatus::asProcessRun));
    }

    private void writeRow(IngestTaskStatus task, TableRow.Builder builder) {
        builder.value(STATE, task.isFinished() ? "FINISHED" : "RUNNING")
                .value(JOB_RUNS, task.getJobRunsOrNull())
                .value(JOB_DURATION, StandardProcessRunReporter.getOrNull(task.getFinishedStatus(),
                        status -> StandardProcessRunReporter.formatDurationString(status.getTimeSpentOnJobs())));
        processRunReporter.writeRunFields(task.asProcessRun(), builder);
    }
}
