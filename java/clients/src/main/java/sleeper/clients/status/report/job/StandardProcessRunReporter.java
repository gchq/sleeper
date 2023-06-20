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

package sleeper.clients.status.report.job;

import sleeper.clients.util.table.TableFieldDefinition;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.record.process.status.ProcessRun;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static sleeper.clients.util.ClientUtils.countWithCommas;
import static sleeper.clients.util.ClientUtils.decimalWithCommas;

public class StandardProcessRunReporter {

    public static final TableFieldDefinition TASK_ID = TableFieldDefinition.field("TASK_ID");
    public static final TableFieldDefinition START_TIME = TableFieldDefinition.field("START_TIME");
    public static final TableFieldDefinition FINISH_TIME = TableFieldDefinition.field("FINISH_TIME");
    public static final TableFieldDefinition DURATION = TableFieldDefinition.numeric("DURATION");
    public static final TableFieldDefinition RECORDS_READ = TableFieldDefinition.numeric("RECORDS_READ");
    public static final TableFieldDefinition RECORDS_WRITTEN = TableFieldDefinition.numeric("RECORDS_WRITTEN");
    public static final TableFieldDefinition READ_RATE = TableFieldDefinition.numeric("READ_RATE (s)");
    public static final TableFieldDefinition WRITE_RATE = TableFieldDefinition.numeric("WRITE_RATE (s)");

    private final PrintStream out;
    public static final String STATE_IN_PROGRESS = "IN PROGRESS";
    public static final String STATE_FINISHED = "FINISHED";

    public StandardProcessRunReporter(PrintStream out, TableWriterFactory.Builder tableBuilder) {
        this(out);
        tableBuilder.addFields(
                TASK_ID, START_TIME, FINISH_TIME, DURATION,
                RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE);
    }

    public StandardProcessRunReporter(PrintStream out) {
        this.out = out;
    }

    public void writeRunFields(ProcessRun run, TableRow.Builder builder) {
        builder.value(TASK_ID, run.getTaskId())
                .value(START_TIME, run.getStartTime())
                .value(FINISH_TIME, run.getFinishTime())
                .value(DURATION, getDurationInSeconds(run))
                .value(RECORDS_READ, getRecordsRead(run))
                .value(RECORDS_WRITTEN, getRecordsWritten(run))
                .value(READ_RATE, getRecordsReadPerSecond(run))
                .value(WRITE_RATE, getRecordsWrittenPerSecond(run));
    }

    public void printProcessJobRun(ProcessRun run) {
        out.println();
        out.printf("Run on task %s%n", run.getTaskId());
        out.printf("Start Time: %s%n", run.getStartTime());
        out.printf("Start Update Time: %s%n", run.getStartUpdateTime());
        if (run.isFinished()) {
            out.printf("Finish Time: %s%n", run.getFinishTime());
            out.printf("Finish Update Time: %s%n", run.getFinishUpdateTime());
            out.printf("Duration: %s%n", getDurationInSeconds(run));
            out.printf("Records Read: %s%n", getRecordsRead(run));
            out.printf("Records Written: %s%n", getRecordsWritten(run));
            out.printf("Read Rate (reads per second): %s%n", getRecordsReadPerSecond(run));
            out.printf("Write Rate (writes per second): %s%n", getRecordsWrittenPerSecond(run));
        } else {
            out.println("Not finished");
        }
    }

    public List<TableFieldDefinition> getFinishedFields() {
        return Arrays.asList(FINISH_TIME, DURATION, RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE);
    }

    public static String getState(ProcessRun run) {
        if (run.isFinished()) {
            return STATE_FINISHED;
        }
        return STATE_IN_PROGRESS;
    }

    public static String getDurationInSeconds(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDurationString((long) (summary.getDurationInSeconds() * 1000)));
    }

    public static String getRecordsRead(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> countWithCommas(summary.getRecordsRead()));
    }

    public static String getRecordsWritten(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> countWithCommas(summary.getRecordsWritten()));
    }

    public static String getRecordsReadPerSecond(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getRecordsReadPerSecond()));
    }

    public static String getRecordsWrittenPerSecond(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getRecordsWrittenPerSecond()));
    }

    public static String formatDecimal(double value) {
        return decimalWithCommas("%.2f", value);
    }

    private static String formatDurationString(long millis) {
        return formatDurationString(Duration.ofMillis(millis));
    }

    public static String formatDurationString(Duration duration) {
        return duration.toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ") // Separate units using a space e.g "1h 2m 3s"
                .toLowerCase(Locale.ROOT);
    }

    public static <I, O> O getOrNull(I object, Function<I, O> getter) {
        if (object == null) {
            return null;
        }
        return getter.apply(object);
    }

}
