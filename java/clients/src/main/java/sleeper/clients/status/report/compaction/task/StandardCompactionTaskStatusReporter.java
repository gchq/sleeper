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
package sleeper.clients.status.report.compaction.task;

import sleeper.clients.status.report.job.AverageRecordRateReport;
import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.job.run.AverageRecordRate;

import java.io.PrintStream;
import java.util.List;

public class StandardCompactionTaskStatusReporter implements CompactionTaskStatusReporter {

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
                        FINISH_TIME, DURATION, JOB_DURATION, JOB_RUNS, RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE)
                .itemsAndWriter(tasks, this::writeRow)
                .build().write(out);
    }

    private void printUnfinishedSummary(List<CompactionTaskStatus> tasks) {
        out.printf("Total tasks in progress: %s%n", tasks.size());
    }

    private void printAllSummary(List<CompactionTaskStatus> tasks) {
        out.printf("Total tasks: %s%n", tasks.size());
        out.printf("Total tasks in progress: %s%n", tasks.stream().filter(task -> !task.isFinished()).count());
        out.printf("Total tasks finished: %s%n", tasks.stream().filter(CompactionTaskStatus::isFinished).count());
        if (tasks.stream().anyMatch(CompactionTaskStatus::isFinished)) {
            out.printf("Total job runs: %s%n", getTotalJobsRun(tasks));
        }
        AverageRecordRateReport.printf("Average compaction rate: %s%n", recordRate(tasks), out);
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
                .value(JOB_RUNS, task.getJobRunsOrNull())
                .value(JOB_DURATION, StandardProcessRunReporter.getOrNull(task.getFinishedStatus(),
                        status -> StandardProcessRunReporter.formatDurationString(status.getTimeSpentOnJobs())));
        processRunReporter.writeRunFields(task.asProcessRun(), builder);
    }
}
