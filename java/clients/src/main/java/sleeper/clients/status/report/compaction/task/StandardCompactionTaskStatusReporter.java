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
package sleeper.clients.status.report.compaction.task;

import sleeper.clients.status.report.job.AverageRecordRateReport;
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.status.report.table.TableField;
import sleeper.clients.status.report.table.TableRow;
import sleeper.clients.status.report.table.TableWriterFactory;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.core.record.process.AverageRecordRate;

import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

public class StandardCompactionTaskStatusReporter implements CompactionTaskStatusReporter {

    private static final TableWriterFactory.Builder TABLE_FACTORY_BUILDER = TableWriterFactory.builder();
    private static final TableField STATE = TABLE_FACTORY_BUILDER.addField("STATE");
    private static final TableField TYPE = TABLE_FACTORY_BUILDER.addField("TYPE");
    private static final TableField TASK_ID = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.TASK_ID);
    private static final TableField START_TIME = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.START_TIME);
    private static final TableField FINISH_TIME = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.FINISH_TIME);
    private static final TableField DURATION = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.DURATION);
    private static final TableField JOB_RUNS = TABLE_FACTORY_BUILDER.addNumericField("JOB_RUNS");
    private static final TableField JOB_DURATION = TABLE_FACTORY_BUILDER.addNumericField("JOB_DURATION");
    private static final TableField LINES_READ = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.LINES_READ);
    private static final TableField LINES_WRITTEN = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.LINES_WRITTEN);
    private static final TableField READ_RATE = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.READ_RATE);
    private static final TableField WRITE_RATE = TABLE_FACTORY_BUILDER.addField(StandardProcessRunReporter.WRITE_RATE);
    private static final TableWriterFactory TABLE_WRITER_FACTORY = TABLE_FACTORY_BUILDER.build();
    private final PrintStream out;
    private final StandardProcessRunReporter processRunReporter;

    public StandardCompactionTaskStatusReporter(PrintStream out) {
        this.processRunReporter = new StandardProcessRunReporter(out);
        this.out = out;
    }

    @Override
    public void report(CompactionTaskQuery query, List<CompactionTaskStatus> tasks) {
        out.println();
        out.println("Compaction Task Status Report");
        out.println("-----------------------------");
        if (query == CompactionTaskQuery.UNFINISHED) {
            printUnfinishedSummary(tasks);
        } else if (query == CompactionTaskQuery.ALL) {
            printAllSummary(tasks);
        }

        TABLE_WRITER_FACTORY.tableBuilder()
                .showFields(query != CompactionTaskQuery.UNFINISHED,
                        FINISH_TIME, DURATION, JOB_DURATION, JOB_RUNS, LINES_READ, LINES_WRITTEN, READ_RATE, WRITE_RATE)
                .itemsAndWriter(tasks, this::writeRow)
                .build().write(out);
    }

    private void printUnfinishedSummary(List<CompactionTaskStatus> tasks) {
        List<CompactionTaskStatus> standardTasks = standardTasks(tasks);
        List<CompactionTaskStatus> splittingTasks = splittingTasks(tasks);
        out.printf("Total tasks in progress: %s%n", tasks.size());
        out.printf("Total standard tasks in progress: %s%n", standardTasks.size());
        out.printf("Total splitting tasks in progress: %s%n", splittingTasks.size());
    }

    private void printAllSummary(List<CompactionTaskStatus> tasks) {
        List<CompactionTaskStatus> standardTasks = standardTasks(tasks);
        List<CompactionTaskStatus> splittingTasks = splittingTasks(tasks);
        out.printf("Total tasks: %s%n", tasks.size());
        out.println();
        out.printf("Total standard tasks: %s%n", standardTasks.size());
        out.printf("Total standard tasks in progress: %s%n", standardTasks.stream().filter(task -> !task.isFinished()).count());
        out.printf("Total standard tasks finished: %s%n", standardTasks.stream().filter(CompactionTaskStatus::isFinished).count());
        if (standardTasks.stream().anyMatch(CompactionTaskStatus::isFinished)) {
            out.printf("Total standard job runs: %s%n", getTotalJobsRun(standardTasks));
        }
        AverageRecordRateReport.printf("Average standard compaction rate: %s%n", recordRate(standardTasks), out);
        out.println();
        out.printf("Total splitting tasks: %s%n", splittingTasks.size());
        out.printf("Total splitting tasks in progress: %s%n", splittingTasks.stream().filter(task -> !task.isFinished()).count());
        out.printf("Total splitting tasks finished: %s%n", splittingTasks.stream().filter(CompactionTaskStatus::isFinished).count());
        if (splittingTasks.stream().anyMatch(CompactionTaskStatus::isFinished)) {
            out.printf("Total splitting job runs: %s%n", getTotalJobsRun(splittingTasks));
        }
        AverageRecordRateReport.printf("Average splitting compaction rate: %s%n", recordRate(splittingTasks), out);
    }

    private static List<CompactionTaskStatus> standardTasks(List<CompactionTaskStatus> tasks) {
        return tasks.stream()
                .filter(task -> task.getType() == CompactionTaskType.COMPACTION)
                .collect(Collectors.toList());
    }

    private static List<CompactionTaskStatus> splittingTasks(List<CompactionTaskStatus> tasks) {
        return tasks.stream()
                .filter(task -> task.getType() == CompactionTaskType.SPLITTING)
                .collect(Collectors.toList());
    }

    private static int getTotalJobsRun(List<CompactionTaskStatus> tasks) {
        return tasks.stream().mapToInt(CompactionTaskStatus::getJobRuns).sum();
    }

    private static AverageRecordRate recordRate(List<CompactionTaskStatus> tasks) {
        return AverageRecordRate.of(tasks.stream()
                .map(CompactionTaskStatus::asProcessRun));
    }

    private void writeRow(CompactionTaskStatus task, TableRow.Builder builder) {
        builder.value(STATE, task.isFinished() ? "FINISHED" : "RUNNING")
                .value(TYPE, task.getType())
                .value(JOB_RUNS, task.getJobRunsOrNull())
                .value(JOB_DURATION, StandardProcessRunReporter.getOrNull(task.getFinishedStatus(),
                        status -> StandardProcessRunReporter.formatDurationString(status.getTimeSpentOnJobs())));
        processRunReporter.writeRunFields(task.asProcessRun(), builder);
    }
}
