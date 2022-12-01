/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.compaction.task;

import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.status.report.job.AverageRecordRateReport;
import sleeper.status.report.job.StandardProcessRunReporter;
import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableRow;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

public class StandardCompactionTaskStatusReporter implements CompactionTaskStatusReporter {

    private final TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
    private final TableField stateField = tableFactoryBuilder.addField("STATE");
    private final TableField typeField = tableFactoryBuilder.addField("TYPE");
    private final TableWriterFactory tableWriterFactory;
    private final PrintStream out;
    private final StandardProcessRunReporter processRunReporter;

    public StandardCompactionTaskStatusReporter(PrintStream out) {
        this.processRunReporter = new StandardProcessRunReporter(out, tableFactoryBuilder);
        tableWriterFactory = tableFactoryBuilder.build();
        this.out = out;
    }

    @Override
    public void report(CompactionTaskQuery query, List<CompactionTaskStatus> tasks) {
        out.println();
        out.println("Compaction Task Status Report");
        out.println("-----------------------------");
        List<CompactionTaskStatus> standardTasks = standardTasks(tasks);
        List<CompactionTaskStatus> splittingTasks = splittingTasks(tasks);
        if (query == CompactionTaskQuery.UNFINISHED) {
            out.printf("Total tasks in progress: %s%n", tasks.size());
            out.printf("Total standard tasks in progress: %s%n", standardTasks.size());
            out.printf("Total splitting tasks in progress: %s%n", splittingTasks.size());
        } else {
            out.printf("Total tasks: %s%n", tasks.size());
            out.println();
            out.printf("Total standard tasks: %s%n", standardTasks.size());
            out.printf("Total standard tasks in progress: %s%n", standardTasks.stream().filter(task -> !task.isFinished()).count());
            out.printf("Total standard tasks finished: %s%n", standardTasks.stream().filter(CompactionTaskStatus::isFinished).count());
            AverageRecordRateReport.printf("Average standard compaction rate: %s%n", recordRate(standardTasks), out);
            out.println();
            out.printf("Total splitting tasks: %s%n", splittingTasks.size());
            out.printf("Total splitting tasks in progress: %s%n", splittingTasks.stream().filter(task -> !task.isFinished()).count());
            out.printf("Total splitting tasks finished: %s%n", splittingTasks.stream().filter(CompactionTaskStatus::isFinished).count());
            AverageRecordRateReport.printf("Average splitting compaction rate: %s%n", recordRate(splittingTasks), out);
        }

        tableWriterFactory.tableBuilder()
                .showFields(query != CompactionTaskQuery.UNFINISHED, processRunReporter.getFinishedFields())
                .itemsAndWriter(tasks, this::writeRow)
                .build().write(out);
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

    private static AverageRecordRate recordRate(List<CompactionTaskStatus> tasks) {
        return AverageRecordRate.of(tasks.stream()
                .map(CompactionTaskStatus::asProcessRun));
    }

    private void writeRow(CompactionTaskStatus task, TableRow.Builder builder) {
        builder.value(stateField, task.isFinished() ? "FINISHED" : "RUNNING")
                .value(typeField, task.getType());
        processRunReporter.writeRunFields(task.asProcessRun(), builder);
    }
}
