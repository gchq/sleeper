/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report.job.query;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * A query to generate a report from a job tracker, for jobs that occurred in a given time period.
 */
public class RangeJobsQuery implements JobQuery {

    public static final String DATE_FORMAT = "yyyyMMddHHmmss";
    private static final Duration DEFAULT_PERIOD = Duration.ofHours(4);

    public static final CommandOption RECENT_COMMAND_OPTION = CommandOption
            .withLongName("recent").shortName('r')
            .helpText("Reports on all jobs in the last 4 hours.")
            .build();
    public static final CommandOption START_COMMAND_OPTION = CommandOption
            .withLongName("start-time").numArgs(NumArgs.ONE)
            .helpText("Start of the period to report on, in the format " + DATE_FORMAT + ".\n" +
                    "Can be combined with --end-time. Defaults to 4 hours before --end-time, or 4 hours before the " +
                    "current time if --end-time is not set.")
            .argsHelpText("<" + DATE_FORMAT + ">")
            .build();
    public static final CommandOption END_COMMAND_OPTION = CommandOption
            .withLongName("end-time").numArgs(NumArgs.ONE)
            .helpText("End of the period to report on, in the format " + DATE_FORMAT + ".\n" +
                    "Can be combined with --start-time. Defaults to the current time.")
            .argsHelpText("<" + DATE_FORMAT + ">")
            .build();

    private final Instant start;
    private final Instant end;

    public RangeJobsQuery(Instant start, Instant end) {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Range end is before range start. Range start: " + start + ", range end: " + end);
        }
        this.start = start;
        this.end = end;
    }

    /**
     * Creates a parser for this query type.
     *
     * @return this parser
     */
    public static JobQueryTypeParser parser() {
        return new JobQueryTypeParser(
                List.of(RECENT_COMMAND_OPTION, START_COMMAND_OPTION, END_COMMAND_OPTION),
                RangeJobsQuery::fromParameters, RangeJobsQuery::fromArguments);
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobTracker tracker, String tableId) {
        return tracker.getJobsInTimePeriod(tableId, start, end);
    }

    @Override
    public List<IngestJobStatus> run(IngestJobTracker tracker, String tableId) {
        return tracker.getJobsInTimePeriod(tableId, start, end);
    }

    @Override
    public JobQueryType getType() {
        return JobQueryType.RANGE;
    }

    /**
     * Reads a command line parameter that sets the time period for a query. Takes the start and end of the period in
     * the format yyyyMMddHHmmss, separated by a comma.
     *
     * @param  queryParameters the start and end of the period as strings separated by a comma, or null for the default
     *                         period
     * @param  timeSupplier    a supplier of the current time (can be fixed for testing)
     * @return                 a query to report on all jobs in the given time period
     */
    private static JobQuery fromParameters(String queryParameters, Supplier<Instant> timeSupplier) {
        if (queryParameters == null) {
            return forDefaultPeriod(timeSupplier);
        } else {
            String[] parts = queryParameters.split(",");
            Instant start = parseStart(parts[0], timeSupplier);
            Instant end = parseEnd(parts[1], timeSupplier);
            return new RangeJobsQuery(start, end);
        }
    }

    private static JobQuery fromArguments(CommandArguments arguments, Supplier<Instant> timeSupplier) {
        Instant end = parseTimeParameter("end-time", arguments).orElseGet(timeSupplier);
        Instant start = parseTimeParameter("start-time", arguments).orElseGet(() -> end.minus(DEFAULT_PERIOD));
        return new RangeJobsQuery(start, end);
    }

    /**
     * Creates a query for the default time period, which is the last 4 hours. Used when a range is asked for without
     * setting the period.
     *
     * @param  timeSupplier a supplier of the current time (can be fixed for testing)
     * @return              a query to report on all jobs in the default time period
     */
    private static JobQuery forDefaultPeriod(Supplier<Instant> timeSupplier) {
        Instant end = timeSupplier.get();
        return new RangeJobsQuery(end.minus(DEFAULT_PERIOD), end);
    }

    /**
     * Prompts the user to set the time period for a query. Will ask for the start and end times as separate prompts in
     * the format yyyyMMddHHmmss.
     *
     * @param  in           the console to prompt the user
     * @param  timeSupplier a supplier of the current time (can be fixed for testing)
     * @return              a query to report on all jobs in the given time period
     */
    public static JobQuery prompt(ConsoleInput in, Supplier<Instant> timeSupplier) {
        Instant start = promptStart(in, timeSupplier);
        Instant end = promptEnd(in, timeSupplier);
        return new RangeJobsQuery(start, end);
    }

    private static Instant promptStart(ConsoleInput in, Supplier<Instant> timeSupplier) {
        String str = in.promptLine("Enter range start in format " + DATE_FORMAT + " (default is 4 hours ago): ");
        try {
            return parseStart(str, timeSupplier);
        } catch (IllegalArgumentException e) {
            return promptStart(in, timeSupplier);
        }
    }

    private static Instant promptEnd(ConsoleInput in, Supplier<Instant> timeSupplier) {
        String str = in.promptLine("Enter range end in format " + DATE_FORMAT + " (default is now): ");
        try {
            return parseEnd(str, timeSupplier);
        } catch (IllegalArgumentException e) {
            return promptEnd(in, timeSupplier);
        }
    }

    private static Instant parseStart(String startStr, Supplier<Instant> timeSupplier) {
        return parseDate(startStr, () -> timeSupplier.get().minus(DEFAULT_PERIOD));
    }

    private static Instant parseEnd(String endStr, Supplier<Instant> timeSupplier) {
        return parseDate(endStr, timeSupplier);
    }

    private static Optional<Instant> parseTimeParameter(String name, CommandArguments arguments) {
        return arguments.getOptionalString(name)
                .map(string -> {
                    try {
                        return parseTime(string);
                    } catch (RuntimeException | ParseException e) {
                        throw new CommandArgumentsException(name + " parameter doesn't match expected format: " + DATE_FORMAT, e);
                    }
                });
    }

    private static Instant parseDate(String input, Supplier<Instant> getDefault) {
        if ("".equals(input)) {
            return getDefault.get();
        }
        try {
            return parseTime(input);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Reads a time set on the command line. Report commands use this to read the start and end of the period to
     * report on, so that the expected format is only defined here. See {@link #DATE_FORMAT} for that format.
     *
     * @param  input                    the time
     * @return                          the time
     * @throws IllegalArgumentException if the time is not in the expected format
     */
    private static Instant parseTime(String input) throws ParseException {
        SimpleDateFormat dateInputFormat = new SimpleDateFormat(DATE_FORMAT);
        dateInputFormat.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        return dateInputFormat.parse(input).toInstant();
    }
}
