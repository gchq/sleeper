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
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.JobQueryPrompt;
import sleeper.clients.report.job.query.JobQueryType;
import sleeper.clients.report.job.query.JobQueryTypeParser;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandOption;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Handling of command line options for reports generated from job trackers. Shared between ingest and compaction job
 * reporting.
 */
public class JobTrackerReportOptions {

    public static final OutputFormatArgument<IngestJobStatusReporter> INGEST_OUTPUT_FORMAT = OutputFormatArgument
            .<IngestJobStatusReporter>withDefault("STANDARD", new StandardIngestJobStatusReporter())
            .addReporter("JSON", new JsonIngestJobStatusReporter())
            .build();

    public static final List<CommandOption> INGEST_OPTIONS = Stream.concat(
            JobQueryType.INGEST_OPTIONS.stream().flatMap(type -> type.options().stream()),
            Stream.of(INGEST_OUTPUT_FORMAT.option()))
            .sorted(Comparator.comparing(CommandOption::longName))
            .toList();

    private static final Map<String, JobQuery> INGEST_PROMPT_EXTRA_QUERIES = Map.of("n", new RejectedJobsQuery());

    private JobTrackerReportOptions() {
    }

    /**
     * Reads the ingest job tracker query requested from the command line.
     *
     * @param  arguments    the command line arguments
     * @param  timeSupplier a supplier of the current time
     * @param  input        the console to prompt the user for further input
     * @return              the query
     */
    public static JobQuery readIngestJobQuery(CommandArguments arguments, Supplier<Instant> timeSupplier, ConsoleInput input) {
        return JobQueryTypeParser.readOneOfTypes(JobQueryType.INGEST_OPTIONS, arguments, timeSupplier)
                .orElseGet(() -> JobQueryPrompt.from(timeSupplier, input, INGEST_PROMPT_EXTRA_QUERIES));
    }

    /**
     * Creates a query for ingest jobs based on parameters. Takes input from the console for the PROMPT query type.
     *
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  timeSupplier    a supplier of the current time
     * @param  input           the console to read from for the PROMPT query type
     * @return                 the query
     */
    public static JobQuery ingestJobQueryFromParametersOrPrompt(JobQueryType queryType, String queryParameters, Supplier<Instant> timeSupplier, ConsoleInput input) {
        return fromParametersOrPrompt(queryType, queryParameters, timeSupplier, input, INGEST_PROMPT_EXTRA_QUERIES);
    }

    /**
     * Creates a query for compaction jobs based on parameters. Takes input from the console for the PROMPT query type.
     *
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  timeSupplier    a supplier of the current time
     * @param  input           the console to read from for the PROMPT query type
     * @return                 the query
     */
    public static JobQuery compactionJobQueryFromParametersOrPrompt(JobQueryType queryType, String queryParameters, Supplier<Instant> timeSupplier, ConsoleInput input) {
        return fromParametersOrPrompt(queryType, queryParameters, timeSupplier, input, Map.of());
    }

    private static JobQuery fromParametersOrPrompt(
            JobQueryType queryType, String queryParameters, Supplier<Instant> timeSupplier, ConsoleInput input, Map<String, JobQuery> extraQueryTypes) {
        if (queryType == JobQueryType.PROMPT) {
            return JobQueryPrompt.from(timeSupplier, input, extraQueryTypes);
        }
        return queryType.parser().read(queryParameters, timeSupplier);
    }

}
