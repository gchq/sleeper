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

/**
 * A helper to create standard output for reports on job runs based on a job tracker. Used for both ingest job and
 * compaction job reporting. This can be used to write a table of job runs, or detailed descriptions.
 */
public class StandardJobRunReporter {

    public static final TableFieldDefinition TASK_ID = TableFieldDefinition.field("TASK_ID");
    public static final TableFieldDefinition START_TIME = TableFieldDefinition.field("START_TIME");
    public static final TableFieldDefinition FINISH_TIME = TableFieldDefinition.field("FINISH_TIME");
    public static final TableFieldDefinition DURATION = TableFieldDefinition.numeric("DURATION");
    public static final TableFieldDefinition ROWS_READ = TableFieldDefinition.numeric("ROWS_READ");
    public static final TableFieldDefinition ROWS_WRITTEN = TableFieldDefinition.numeric("ROWS_WRITTEN");
    public static final TableFieldDefinition READ_RATE = TableFieldDefinition.numeric("READ_RATE (s)");
    public static final TableFieldDefinition WRITE_RATE = TableFieldDefinition.numeric("WRITE_RATE (s)");
    public static final TableFieldDefinition FAILURE_REASONS = TableFieldDefinition.field("FAILURE_REASONS");

    private final PrintStream out;

    /**
     * Creates a builder to add the standard job run fields to a given table. This can be used to add your own fields
     * to the table writer to combine with the standard fields in a given order.
     *
     * @param  tableBuilder the builder for a writer for the table
     * @return              the builder for the job run reporter
     */
    public static Builder withTable(TableWriterFactory.Builder tableBuilder) {
        return new Builder(tableBuilder);
    }

    /**
     * Creates a reporter to write to a given output.
     *
     * @param out the output
     */
    public StandardJobRunReporter(PrintStream out) {
        this.out = out;
    }

    /**
     * Populates values for the standard job run fields in a row of the table. This can be combined with your own code
     * to set values for other fields as well.
     *
     * @param run     the job run to be written to the table row
     * @param builder the builder for the table row
     */
    public void writeRunFields(JobRunReport run, TableRow.Builder builder) {
        builder.value(TASK_ID, run.getTaskId())
                .value(START_TIME, run.getStartTime())
                .value(FINISH_TIME, run.getFinishTime());
        if (run.isFinished()) {
            JobRunSummary summary = run.getFinishedSummary();
            builder.value(DURATION, getDurationString(summary))
                    .value(ROWS_READ, getRowsRead(summary))
                    .value(ROWS_WRITTEN, getRowsWritten(summary));
            if (!summary.getDuration().isZero()) {
                builder.value(READ_RATE, getRowsReadPerSecond(summary))
                        .value(WRITE_RATE, getRowsWrittenPerSecond(summary));
            }
        }
    }

    /**
     * Prints a detailed description of a job run. Allows customisation of how individual job status updates are
     * printed, by setting update printers, e.g. with {@link #updatePrinters()}, {@link #printUpdateType()},
     * {@link #printUpdateTypeInRun()}. Specified printers will be combined with the default, so it is not necessary to
     * handle update types common to all job runs.
     *
     * @param run           the job run to print
     * @param updatePrinter the update printer to print individual job status updates in the run
     */
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

    /**
     * Creates an update printer to print job status updates of a certain type, including information about the run it
     * occurred in. This will be used for any status update of this type. Can be combined with other update printers
     * with {@link #updatePrinters()}.
     *
     * @param  <T>     the status update type
     * @param  type    the status update type
     * @param  printer the method to print the update to the report, taking the run the update happened in as well as
     *                 the update
     * @return         the update printer
     */
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

    /**
     * Creates an update printer to print job status updates of a certain type. This will be used for any status update
     * of this type. Can be combined with other update printers with {@link #updatePrinters()}.
     *
     * @param  <T>     the status update type
     * @param  type    the status update type
     * @param  printer the method to print the update to the report
     * @return         the update printer
     */
    public static <T extends JobStatusUpdate> UpdatePrinter printUpdateType(Class<T> type, Consumer<T> printer) {
        return printUpdateTypeInRun(type, (run, update) -> printer.accept(update));
    }

    /**
     * Creates an update printer that combines other update printers together. Uses the first printer that can process a
     * given job status update.
     *
     * @param  printers the update printers to combine
     * @return          the update printer
     */
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

    /**
     * An update printer that prints job status updates to a standard report. Can be combined with other update printers
     * to handle different update types, see {@link #updatePrinters()}.
     */
    public interface UpdatePrinter {

        /**
         * Prints a job status update to a report.
         *
         * @param  run    the run that the job status occurred in
         * @param  update the job status update to print
         * @return        true if the update was printed, false if the printer cannot handle this update
         */
        boolean print(JobRunReport run, JobStatusUpdate update);
    }

    private void printProcessStarted(JobRunStartedUpdate update) {
        out.printf("Start time: %s%n", update.getStartTime());
        out.printf("Start update time: %s%n", update.getUpdateTime());
    }

    private void printProcessFinished(JobRunReport run, JobRunEndUpdate update) {
        JobRunSummary summary = run.getFinishedSummary();
        out.printf("Finish time: %s%n", summary.getFinishTime());
        out.printf("Finish update time: %s%n", update.getUpdateTime());
        out.printf("Duration: %s%n", getDurationString(summary)); // Duration from job started in driver or job accepted in executor?
        if (update.isSuccessful()) {
            out.printf("Rows read: %s%n", getRowsRead(summary));
            out.printf("Rows written: %s%n", getRowsWritten(summary));
            if (!summary.getDuration().isZero()) {
                out.printf("Read rate (reads per second): %s%n", getRowsReadPerSecond(summary));
                out.printf("Write rate (writes per second): %s%n", getRowsWrittenPerSecond(summary));
            }
        } else {
            out.println("Run failed, reasons:");
            update.getFailureReasons()
                    .forEach(reason -> out.printf("- %s%n", reason));
        }
    }

    public List<TableFieldDefinition> getFinishedFields() {
        return Arrays.asList(DURATION, ROWS_READ, ROWS_WRITTEN, READ_RATE, WRITE_RATE);
    }

    private static String getDurationString(JobRunSummary summary) {
        return formatDurationString(summary.getDuration());
    }

    private static String getRowsRead(JobRunSummary summary) {
        return countWithCommas(summary.getRowsRead());
    }

    private static String getRowsWritten(JobRunSummary summary) {
        return countWithCommas(summary.getRowsWritten());
    }

    private static String getRowsReadPerSecond(JobRunSummary summary) {
        return formatDecimal2dp(summary.getRowsReadPerSecond());
    }

    private static String getRowsWrittenPerSecond(JobRunSummary summary) {
        return formatDecimal2dp(summary.getRowsWrittenPerSecond());
    }

    /**
     * Formats a duration to be included in a standard report.
     *
     * @param  duration the duration
     * @return          the formatted string
     */
    public static String formatDurationString(Duration duration) {
        return duration.toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ") // Separate units using a space e.g "1h 2m 3s"
                .toLowerCase(Locale.ROOT);
    }

    /**
     * Retrieves a value if a given object is not null. This is usually used when setting the value of a field in a
     * table, to set the field to null if the object is null.
     *
     * @param  <I>    the type of object containing the value to retrieve
     * @param  <O>    the type of the value to retrieve
     * @param  object the object containing the value to retrieve
     * @param  getter a method to retrieve the value from the object
     * @return        the value, or null if the object was null
     */
    public static <I, O> O getOrNull(I object, Function<I, O> getter) {
        if (object == null) {
            return null;
        }
        return getter.apply(object);
    }

    /**
     * A builder to add the standard job run fields to a given table. This can be combined with your own code
     * to set values for other fields as well.
     */
    public static class Builder {
        private TableWriterFactory.Builder tableBuilder;

        private Builder(TableWriterFactory.Builder tableBuilder) {
            this.tableBuilder = tableBuilder;
        }

        /**
         * Adds the standard fields for progress of a job run.
         *
         * @return this builder
         */
        public Builder addProgressFields() {
            tableBuilder.addFields(TASK_ID, START_TIME, FINISH_TIME);
            return this;
        }

        /**
         * Adds the standard fields for the results of a job run.
         *
         * @return this builder
         */
        public Builder addResultsFields() {
            tableBuilder.addFields(DURATION, ROWS_READ, ROWS_WRITTEN, READ_RATE, WRITE_RATE, FAILURE_REASONS);
            return this;
        }

        public StandardJobRunReporter build(PrintStream out) {
            return new StandardJobRunReporter(out);
        }
    }
}
