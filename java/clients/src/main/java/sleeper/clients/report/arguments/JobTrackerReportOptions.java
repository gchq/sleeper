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
package sleeper.clients.report.arguments;

import sleeper.clients.report.ingest.job.IngestJobStatusReporter;
import sleeper.clients.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.job.query.AllJobsQuery;
import sleeper.clients.report.job.query.DetailedJobsQuery;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.JobQueryPrompt;
import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.report.job.query.UnfinishedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class JobTrackerReportOptions {

    /**
     * The query type options, and the query type each one selects. Declared in the order they appear in the usage,
     * which is the order they are reported in if the user sets more than one.
     */
    private static final Map<String, JobQuery.Type> QUERY_TYPE_BY_OPTION = Map.of(
            AllJobsQuery.COMMAND_OPTION.longName(), JobQuery.Type.ALL,
            DetailedJobsQuery.COMMAND_OPTION.longName(), JobQuery.Type.DETAILED,
            RangeJobsQuery.COMMAND_OPTION.longName(), JobQuery.Type.RANGE,
            RejectedJobsQuery.COMMAND_OPTION.longName(), JobQuery.Type.REJECTED,
            UnfinishedJobsQuery.COMMAND_OPTION.longName(), JobQuery.Type.UNFINISHED);

    public static final ReportTypeArgument<IngestJobStatusReporter> INGEST_REPORT_TYPE = ReportTypeArgument
            .<IngestJobStatusReporter>withDefault("STANDARD", new StandardIngestJobStatusReporter())
            .addReporter("JSON", new JsonIngestJobStatusReporter())
            .build();

    public static List<CommandOption> INGEST_OPTIONS = Stream.of(
            AllJobsQuery.COMMAND_OPTION,
            DetailedJobsQuery.COMMAND_OPTION,
            RangeJobsQuery.COMMAND_OPTION,
            RangeJobsQuery.START_COMMAND_OPTION,
            RangeJobsQuery.END_COMMAND_OPTION,
            RejectedJobsQuery.COMMAND_OPTION,
            UnfinishedJobsQuery.COMMAND_OPTION,
            INGEST_REPORT_TYPE.option())
            .sorted(Comparator.comparing(CommandOption::longName))
            .toList();

    public static JobQuery readIngestJobQuery(CommandArguments arguments, Clock clock, ConsoleInput input) {
        return readJobQuery(arguments, clock, input, INGEST_OPTIONS, Map.of("n", new RejectedJobsQuery()));
    }

    private static JobQuery readJobQuery(CommandArguments arguments, Clock clock, ConsoleInput input, List<CommandOption> options, Map<String, JobQuery> extraQueryTypes) {
        JobQuery.Type jobType = determineQueryType(arguments, options);
        String jobId = null;
        Instant startTime = null;
        Instant endTime = null;

        switch (jobType) {
            case DETAILED:
                jobId = arguments.getString("detailed");
                if (jobId.isEmpty()) {
                    throw new CommandArgumentsException("Expected a value for option: detailed");
                }
                break;
            case RANGE:
                Optional<String> optionalStart = arguments.getOptionalString("start-time");
                Optional<String> optionalEnd = arguments.getOptionalString("end-time");

                if (optionalStart.isPresent() && optionalEnd.isPresent()) {
                    startTime = readTime("start-time", optionalStart.get());
                    endTime = readTime("end-time", optionalEnd.get());
                    if (endTime.isBefore(startTime)) {
                        throw new CommandArgumentsException("Range end is before range start. Range start: " + optionalStart.get() + ", range end: " + optionalEnd.get());
                    }
                } else if (optionalStart.isEmpty() && optionalEnd.isPresent()) {
                    throw new CommandArgumentsException("Missing parameter of start-time which is required for the Range query type.");
                } else if (optionalStart.isPresent() && optionalEnd.isEmpty()) {
                    throw new CommandArgumentsException("Missing parameter of end-time which is required for the Range query type.");
                }
                break;
            default:
                break;
        }

        // Below error message to be removed as part of work for ticket number: https://github.com/gchq/sleeper/issues/8061
        if (!jobType.equals(JobQuery.Type.RANGE) &&
                (arguments.getOptionalString("start-time").isPresent() ||
                        arguments.getOptionalString("end-time").isPresent())) {
            throw new CommandArgumentsException("Range time flags, start-time and end-time are not valid for following query type: " + jobType);
        }

        JobQuery query;
        if (jobType == JobQuery.Type.RANGE) {
            if (startTime == null) {
                query = RangeJobsQuery.forDefaultPeriod(clock);
            } else {
                query = new RangeJobsQuery(startTime, endTime);
            }
        } else if (jobType == JobQuery.Type.PROMPT) {
            return JobQueryPrompt.from(clock, input, extraQueryTypes);
        } else {
            query = JobQuery.from(jobType, jobId, clock);
        }
        return query;
    }

    /**
     * Determines which query type the user asked for. Exactly one query type option may be set. If none is set, the
     * user is prompted for one, unless a time was given for a range.
     *
     * @param  arguments the parsed command line arguments
     * @return           the query type
     */
    private static JobQuery.Type determineQueryType(CommandArguments arguments, List<CommandOption> options) {
        List<JobQuery.Type> setTypes = options.stream()
                .filter(arguments::isSet)
                .map(CommandOption::longName)
                .filter(QUERY_TYPE_BY_OPTION::containsKey)
                .map(QUERY_TYPE_BY_OPTION::get)
                .toList();
        if (setTypes.size() > 1) {
            throw new CommandArgumentsException("Too many query type flags are set, maximum of 1. Flags set: " +
                    setTypes.stream().map(JobQuery.Type::name).collect(joining(", ")));
        }
        if (!setTypes.isEmpty()) {
            return setTypes.get(0);
        }
        // Additional step to trigger range query if no flag presented, but start-time or end-time present.
        // Either one on its own is an error, but it is reported when the range is read, so that the user is told
        // which one is missing rather than that the time they did set is invalid for some other query type.
        // Likely to be refactored when including range as an option with the Query Types rather than a separate one
        // See ticket: https://github.com/gchq/sleeper/issues/8061
        if (arguments.getOptionalString("start-time").isPresent()
                || arguments.getOptionalString("end-time").isPresent()) {
            return JobQuery.Type.RANGE;
        }
        return JobQuery.Type.PROMPT;
    }

    private static Instant readTime(String option, String value) {
        try {
            return RangeJobsQuery.parseTime(value);
        } catch (IllegalArgumentException e) {
            throw new CommandArgumentsException(
                    option + " parameter doesn't match expected format: " + RangeJobsQuery.DATE_FORMAT);
        }
    }

}
