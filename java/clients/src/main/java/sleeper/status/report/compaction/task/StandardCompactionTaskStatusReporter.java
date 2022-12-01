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
import sleeper.core.record.process.AverageRecordRate;
import sleeper.status.report.job.AverageRecordRateReport;
import sleeper.status.report.job.StandardProcessRunReporter;
import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableRow;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;
import java.util.List;

public class StandardCompactionTaskStatusReporter implements CompactionTaskStatusReporter {

    private final TableWriterFactory.Builder tableFactoryBuilder = TableWriterFactory.builder();
    private final TableField stateField = tableFactoryBuilder.addField("STATE");
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
        if (query == CompactionTaskQuery.UNFINISHED) {
            out.printf("Total unfinished tasks: %s%n", tasks.size());
        } else {
            out.printf("Total tasks: %s%n", tasks.size());
            out.printf("Total unfinished tasks: %s%n", tasks.stream().filter(task -> !task.isFinished()).count());
            out.printf("Total finished tasks: %s%n", tasks.stream().filter(CompactionTaskStatus::isFinished).count());
            AverageRecordRateReport.printf("Average compaction rate: %s%n", recordRate(tasks), out);
        }

        tableWriterFactory.tableBuilder()
                .showFields(query != CompactionTaskQuery.UNFINISHED, processRunReporter.getFinishedFields())
                .itemsAndWriter(tasks, this::writeRow)
                .build().write(out);
    }

    private static AverageRecordRate recordRate(List<CompactionTaskStatus> tasks) {
        return AverageRecordRate.of(tasks.stream()
                .map(CompactionTaskStatus::asProcessRun));
    }

    private void writeRow(CompactionTaskStatus task, TableRow.Builder builder) {
        builder.value(stateField, task.isFinished() ? "FINISHED" : "RUNNING");
        processRunReporter.writeRunFields(task.asProcessRun(), builder);
    }
}
