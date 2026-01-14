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
package sleeper.clients.report.compaction.task;

import sleeper.clients.report.job.AverageRowRateReport;
import sleeper.clients.report.job.StandardJobRunReporter;
import sleeper.clients.util.tablewriter.TableField;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.job.run.AverageRowRate;

import java.io.PrintStream;
import java.util.List;

/**
 * Creates reports in human-readable string format on the status of compaction tasks. This produces a table.
 */
public class StandardCompactionTaskStatusReporter implements CompactionTaskStatusReporter {

    private static final TableWriterFactory.Builder TABLE_FACTORY_BUILDER = TableWriterFactory.builder();
    private static final TableField STATE = TABLE_FACTORY_BUILDER.addField("STATE");
    private static final TableField TASK_ID = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.TASK_ID);
    private static final TableField START_TIME = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.START_TIME);
    private static final TableField FINISH_TIME = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.FINISH_TIME);
    private static final TableField DURATION = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.DURATION);
    private static final TableField JOB_RUNS = TABLE_FACTORY_BUILDER.addNumericField("JOB_RUNS");
    private static final TableField JOB_DURATION = TABLE_FACTORY_BUILDER.addNumericField("JOB_DURATION");
    private static final TableField ROWS_READ = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.ROWS_READ);
    private static final TableField ROWS_WRITTEN = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.ROWS_WRITTEN);
    private static final TableField READ_RATE = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.READ_RATE);
    private static final TableField WRITE_RATE = TABLE_FACTORY_BUILDER.addField(StandardJobRunReporter.WRITE_RATE);
    private static final TableWriterFactory TABLE_WRITER_FACTORY = TABLE_FACTORY_BUILDER.build();
    private final PrintStream out;
    private final StandardJobRunReporter jobRunReporter;

    public StandardCompactionTaskStatusReporter(PrintStream out) {
        this.jobRunReporter = new StandardJobRunReporter(out);
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
                        FINISH_TIME, DURATION, JOB_DURATION, JOB_RUNS, ROWS_READ, ROWS_WRITTEN, READ_RATE, WRITE_RATE)
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
        AverageRowRateReport.printf("Average compaction rate: %s%n", rowRate(tasks), out);
    }

    private static int getTotalJobsRun(List<CompactionTaskStatus> tasks) {
        return tasks.stream().mapToInt(CompactionTaskStatus::getJobRuns).sum();
    }

    private static AverageRowRate rowRate(List<CompactionTaskStatus> tasks) {
        return AverageRowRate.of(tasks.stream()
                .map(CompactionTaskStatus::asJobRunReport));
    }

    private void writeRow(CompactionTaskStatus task, TableRow.Builder builder) {
        builder.value(STATE, task.isFinished() ? "FINISHED" : "RUNNING")
                .value(JOB_RUNS, task.getJobRunsOrNull())
                .value(JOB_DURATION, StandardJobRunReporter.getOrNull(task.getFinishedStatus(),
                        status -> StandardJobRunReporter.formatDurationString(status.getTimeSpentOnJobs())));
        jobRunReporter.writeRunFields(task.asJobRunReport(), builder);
    }
}
