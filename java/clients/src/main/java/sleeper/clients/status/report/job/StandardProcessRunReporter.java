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

package sleeper.clients.status.report.job;

import sleeper.clients.util.table.TableFieldDefinition;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRunFinishedUpdate;
import sleeper.core.record.process.status.ProcessRunStartedUpdate;
import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Function;

import static sleeper.core.util.NumberFormatUtils.countWithCommas;
import static sleeper.core.util.NumberFormatUtils.decimalWithCommas;

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
                .value(FINISH_TIME, run.getFinishTime());
        if (run.isFinished()) {
            RecordsProcessedSummary summary = run.getFinishedSummary();
            builder.value(DURATION, getDurationString(summary))
                    .value(RECORDS_READ, getRecordsRead(summary))
                    .value(RECORDS_WRITTEN, getRecordsWritten(summary))
                    .value(READ_RATE, getRecordsReadPerSecond(summary))
                    .value(WRITE_RATE, getRecordsWrittenPerSecond(summary));
        }
    }

    public void printProcessJobRunWithUpdatePrinter(ProcessRun run, UpdatePrinter updatePrinter) {
        printProcessJobRun(run, updatePrinters(updatePrinter, defaultUpdatePrinter()));
    }

    private UpdatePrinter defaultUpdatePrinter() {
        return updatePrinters(
                printUpdateType(ProcessRunStartedUpdate.class, this::printProcessStarted),
                printUpdateType(ProcessRunFinishedUpdate.class, this::printProcessFinished));
    }

    private void printProcessJobRun(ProcessRun run, UpdatePrinter updatePrinter) {
        out.println();
        if (run.getTaskId() != null) {
            out.printf("Run on task %s%n", run.getTaskId());
        }
        for (ProcessStatusUpdate update : run.getStatusUpdates()) {
            if (!updatePrinter.print(update)) {
                out.printf("Unknown update type: %s%n", update.getClass().getSimpleName());
            }
        }
    }

    public static <T extends ProcessStatusUpdate> UpdatePrinter printUpdateType(Class<T> type, Consumer<T> printer) {
        return update -> {
            if (type.isInstance(update)) {
                printer.accept(type.cast(update));
                return true;
            } else {
                return false;
            }
        };
    }

    public static UpdatePrinter updatePrinters(UpdatePrinter... printers) {
        return update -> {
            for (UpdatePrinter printer : printers) {
                if (printer.print(update)) {
                    return true;
                }
            }
            return false;
        };
    }

    public interface UpdatePrinter {
        boolean print(ProcessStatusUpdate update);
    }

    public void printProcessStarted(ProcessRunStartedUpdate update) {
        out.printf("Start time: %s%n", update.getStartTime());
        out.printf("Start update time: %s%n", update.getUpdateTime());
    }

    public void printProcessFinished(ProcessRunFinishedUpdate update) {
        RecordsProcessedSummary summary = update.getSummary();
        out.printf("Finish time: %s%n", summary.getFinishTime());
        out.printf("Finish update time: %s%n", update.getUpdateTime());
        out.printf("Duration: %s%n", getDurationString(summary)); // Duration from job started in driver or job accepted in executor?
        if (update.isSuccessful()) {
            out.printf("Records read: %s%n", getRecordsRead(summary));
            out.printf("Records written: %s%n", getRecordsWritten(summary));
            out.printf("Read rate (reads per second): %s%n", getRecordsReadPerSecond(summary));
            out.printf("Write rate (writes per second): %s%n", getRecordsWrittenPerSecond(summary));
        } else {
            out.println("Run failed, reasons:");
            update.getFailureReasons()
                    .forEach(reason -> out.printf("- %s%n", reason));
        }
    }

    public List<TableFieldDefinition> getFinishedFields() {
        return Arrays.asList(FINISH_TIME, DURATION, RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE);
    }

    private static String getDurationString(RecordsProcessedSummary summary) {
        return formatDurationString(summary.getDuration());
    }

    private static String getRecordsRead(RecordsProcessedSummary summary) {
        return countWithCommas(summary.getRecordsRead());
    }

    private static String getRecordsWritten(RecordsProcessedSummary summary) {
        return countWithCommas(summary.getRecordsWritten());
    }

    private static String getRecordsReadPerSecond(RecordsProcessedSummary summary) {
        return formatDecimal(summary.getRecordsReadPerSecond());
    }

    private static String getRecordsWrittenPerSecond(RecordsProcessedSummary summary) {
        return formatDecimal(summary.getRecordsWrittenPerSecond());
    }

    public static String formatDecimal(double value) {
        return decimalWithCommas("%.2f", value);
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
