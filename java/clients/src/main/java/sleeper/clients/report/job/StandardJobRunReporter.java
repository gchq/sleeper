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

package sleeper.clients.report.job;

import sleeper.clients.util.tablewriter.TableFieldDefinition;
import sleeper.clients.util.tablewriter.TableRow;
import sleeper.clients.util.tablewriter.TableWriterFactory;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.status.JobRunEndUpdate;
import sleeper.core.tracker.job.status.JobRunStartedUpdate;
import sleeper.core.tracker.job.status.JobStatusUpdate;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static sleeper.core.util.NumberFormatUtils.countWithCommas;
import static sleeper.core.util.NumberFormatUtils.formatDecimal2dp;

public class StandardJobRunReporter {

    public static final TableFieldDefinition TASK_ID = TableFieldDefinition.field("TASK_ID");
    public static final TableFieldDefinition START_TIME = TableFieldDefinition.field("START_TIME");
    public static final TableFieldDefinition FINISH_TIME = TableFieldDefinition.field("FINISH_TIME");
    public static final TableFieldDefinition DURATION = TableFieldDefinition.numeric("DURATION");
    public static final TableFieldDefinition RECORDS_READ = TableFieldDefinition.numeric("RECORDS_READ");
    public static final TableFieldDefinition RECORDS_WRITTEN = TableFieldDefinition.numeric("RECORDS_WRITTEN");
    public static final TableFieldDefinition READ_RATE = TableFieldDefinition.numeric("READ_RATE (s)");
    public static final TableFieldDefinition WRITE_RATE = TableFieldDefinition.numeric("WRITE_RATE (s)");
    public static final TableFieldDefinition FAILURE_REASONS = TableFieldDefinition.field("FAILURE REASONS");

    private final PrintStream out;

    public static Builder withTable(TableWriterFactory.Builder tableBuilder) {
        return new Builder(tableBuilder);
    }

    public StandardJobRunReporter(PrintStream out, TableWriterFactory.Builder tableBuilder) {
        this(out);
        tableBuilder.addFields(
                TASK_ID, START_TIME, FINISH_TIME, DURATION,
                RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE, FAILURE_REASONS);
    }

    public StandardJobRunReporter(PrintStream out) {
        this.out = out;
    }

    public void writeRunFields(JobRunReport run, TableRow.Builder builder) {
        builder.value(TASK_ID, run.getTaskId())
                .value(START_TIME, run.getStartTime())
                .value(FINISH_TIME, run.getFinishTime());
        if (run.isFinished()) {
            JobRunSummary summary = run.getFinishedSummary();
            builder.value(DURATION, getDurationString(summary))
                    .value(RECORDS_READ, getRecordsRead(summary))
                    .value(RECORDS_WRITTEN, getRecordsWritten(summary));
            if (!summary.getDuration().isZero()) {
                builder.value(READ_RATE, getRecordsReadPerSecond(summary))
                        .value(WRITE_RATE, getRecordsWrittenPerSecond(summary));
            }
        }
    }

    public void printProcessJobRunWithUpdatePrinter(JobRunReport run, UpdatePrinter updatePrinter) {
        printProcessJobRun(run, updatePrinters(updatePrinter, defaultUpdatePrinter()));
    }

    private UpdatePrinter defaultUpdatePrinter() {
        return updatePrinters(
                printUpdateType(JobRunStartedUpdate.class, this::printProcessStarted),
                printUpdateTypeInRun(JobRunEndUpdate.class, this::printProcessFinished));
    }

    private void printProcessJobRun(JobRunReport run, UpdatePrinter updatePrinter) {
        out.println();
        if (run.getTaskId() != null) {
            out.printf("Run on task %s%n", run.getTaskId());
        }
        for (JobStatusUpdate update : run.getStatusUpdates()) {
            if (!updatePrinter.print(run, update)) {
                out.printf("Unknown update type: %s%n", update.getClass().getSimpleName());
            }
        }
    }

    public static <T extends JobStatusUpdate> UpdatePrinter printUpdateTypeInRun(Class<T> type, BiConsumer<JobRunReport, T> printer) {
        return (run, update) -> {
            if (type.isInstance(update)) {
                printer.accept(run, type.cast(update));
                return true;
            } else {
                return false;
            }
        };
    }

    public static <T extends JobStatusUpdate> UpdatePrinter printUpdateType(Class<T> type, Consumer<T> printer) {
        return printUpdateTypeInRun(type, (run, update) -> printer.accept(update));
    }

    public static UpdatePrinter updatePrinters(UpdatePrinter... printers) {
        return (run, update) -> {
            for (UpdatePrinter printer : printers) {
                if (printer.print(run, update)) {
                    return true;
                }
            }
            return false;
        };
    }

    public interface UpdatePrinter {
        boolean print(JobRunReport run, JobStatusUpdate update);
    }

    public void printProcessStarted(JobRunStartedUpdate update) {
        out.printf("Start time: %s%n", update.getStartTime());
        out.printf("Start update time: %s%n", update.getUpdateTime());
    }

    public void printProcessFinished(JobRunReport run, JobRunEndUpdate update) {
        JobRunSummary summary = run.getFinishedSummary();
        out.printf("Finish time: %s%n", summary.getFinishTime());
        out.printf("Finish update time: %s%n", update.getUpdateTime());
        out.printf("Duration: %s%n", getDurationString(summary)); // Duration from job started in driver or job accepted in executor?
        if (update.isSuccessful()) {
            out.printf("Records read: %s%n", getRecordsRead(summary));
            out.printf("Records written: %s%n", getRecordsWritten(summary));
            if (!summary.getDuration().isZero()) {
                out.printf("Read rate (reads per second): %s%n", getRecordsReadPerSecond(summary));
                out.printf("Write rate (writes per second): %s%n", getRecordsWrittenPerSecond(summary));
            }
        } else {
            out.println("Run failed, reasons:");
            update.getFailureReasons()
                    .forEach(reason -> out.printf("- %s%n", reason));
        }
    }

    public List<TableFieldDefinition> getFinishedFields() {
        return Arrays.asList(FINISH_TIME, DURATION, RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE);
    }

    public List<TableFieldDefinition> getUnfinishedFields() {
        return Arrays.asList(FAILURE_REASONS);
    }

    private static String getDurationString(JobRunSummary summary) {
        return formatDurationString(summary.getDuration());
    }

    private static String getRecordsRead(JobRunSummary summary) {
        return countWithCommas(summary.getRecordsRead());
    }

    private static String getRecordsWritten(JobRunSummary summary) {
        return countWithCommas(summary.getRecordsWritten());
    }

    private static String getRecordsReadPerSecond(JobRunSummary summary) {
        return formatDecimal2dp(summary.getRecordsReadPerSecond());
    }

    private static String getRecordsWrittenPerSecond(JobRunSummary summary) {
        return formatDecimal2dp(summary.getRecordsWrittenPerSecond());
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

    public static class Builder {
        private TableWriterFactory.Builder tableBuilder;

        public Builder(TableWriterFactory.Builder tableBuilder) {
            this.tableBuilder = tableBuilder;
        }

        public Builder addProgressFields() {
            tableBuilder.addFields(TASK_ID, START_TIME, FINISH_TIME);
            return this;
        }

        public Builder addResultsFields() {
            tableBuilder.addFields(DURATION, RECORDS_READ, RECORDS_WRITTEN, READ_RATE, WRITE_RATE, FAILURE_REASONS);
            return this;
        }

        public StandardJobRunReporter build(PrintStream out) {
            return new StandardJobRunReporter(out);
        }
    }
}
