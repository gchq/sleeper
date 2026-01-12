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
package sleeper.clients.report.job.query;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * A query to generate a report from a job tracker, for jobs that occurred in a given time period.
 */
public class RangeJobsQuery implements JobQuery {

    public static final String DATE_FORMAT = "yyyyMMddHHmmss";

    private final String tableId;
    private final Instant start;
    private final Instant end;

    public RangeJobsQuery(TableStatus table, Instant start, Instant end) {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Start of range provided is after end");
        }
        this.tableId = table.getTableUniqueId();
        this.start = start;
        this.end = end;
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobTracker tracker) {
        return tracker.getJobsInTimePeriod(tableId, start, end);
    }

    @Override
    public List<IngestJobStatus> run(IngestJobTracker tracker) {
        return tracker.getJobsInTimePeriod(tableId, start, end);
    }

    @Override
    public Type getType() {
        return Type.RANGE;
    }

    /**
     * Reads a command line parameter that sets the time period for a query. Takes the start and end of the period in
     * the format yyyyMMddHHmmss, separated by a comma.
     *
     * @param  table           the Sleeper table to be queried
     * @param  queryParameters the start and end of the period as strings separated by a comma, or null for the default
     *                         period
     * @param  clock           a clock to get the current time (can be fixed for testing)
     * @return                 a query to report on all jobs in the given time period
     */
    public static JobQuery fromParameters(TableStatus table, String queryParameters, Clock clock) {
        if (queryParameters == null) {
            Instant end = clock.instant();
            Instant start = end.minus(Duration.ofHours(4));
            return new RangeJobsQuery(table, start, end);
        } else {
            String[] parts = queryParameters.split(",");
            Instant start = parseStart(parts[0], clock);
            Instant end = parseEnd(parts[1], clock);
            return new RangeJobsQuery(table, start, end);
        }
    }

    /**
     * Prompts the user to set the time period for a query. Will ask for the start and end times as separate prompts in
     * the format yyyyMMddHHmmss.
     *
     * @param  table the Sleeper table to be queried
     * @param  in    the console to prompt the user
     * @param  clock a clock to get the current time (can be fixed for testing)
     * @return       a query to report on all jobs in the given time period
     */
    public static JobQuery prompt(TableStatus table, ConsoleInput in, Clock clock) {
        Instant start = promptStart(in, clock);
        Instant end = promptEnd(in, clock);
        return new RangeJobsQuery(table, start, end);
    }

    private static Instant promptStart(ConsoleInput in, Clock clock) {
        String str = in.promptLine("Enter range start in format " + DATE_FORMAT + " (default is 4 hours ago): ");
        try {
            return parseStart(str, clock);
        } catch (IllegalArgumentException e) {
            return promptStart(in, clock);
        }
    }

    private static Instant promptEnd(ConsoleInput in, Clock clock) {
        String str = in.promptLine("Enter range end in format " + DATE_FORMAT + " (default is now): ");
        try {
            return parseEnd(str, clock);
        } catch (IllegalArgumentException e) {
            return promptEnd(in, clock);
        }
    }

    private static Instant parseStart(String startStr, Clock clock) {
        return parseDate(startStr, () -> clock.instant().minus(Duration.ofHours(4)));
    }

    private static Instant parseEnd(String endStr, Clock clock) {
        return parseDate(endStr, clock::instant);
    }

    private static Instant parseDate(String input, Supplier<Instant> getDefault) {
        if ("".equals(input)) {
            return getDefault.get();
        }
        try {
            return createDateInputFormat().parse(input).toInstant();
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static SimpleDateFormat createDateInputFormat() {
        SimpleDateFormat dateInputFormat = new SimpleDateFormat(DATE_FORMAT);
        dateInputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateInputFormat;
    }
}
