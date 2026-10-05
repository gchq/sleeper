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

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

/**
 * A query to generate a report based on jobs in a job tracker. Different types of query can include jobs based on their
 * status or other parameters.
 */
public interface JobQuery {

    /**
     * Retrieves compaction jobs matching this query.
     *
     * @param  tracker the job tracker
     * @param  tableId the Sleeper table ID to report on
     * @return         the jobs
     */
    List<CompactionJobStatus> run(CompactionJobTracker tracker, String tableId);

    /**
     * Retrieves ingest jobs matching this query.
     *
     * @param  tracker the job tracker
     * @param  tableId the Sleeper table ID to report on
     * @return         the jobs
     */
    List<IngestJobStatus> run(IngestJobTracker tracker, String tableId);

    /**
     * Retrieves the type of this query.
     *
     * @return the query type
     */
    JobQueryType getType();

    /**
     * Creates a query for jobs based on command line arguments for an ingest jobs report. If none is specified, an
     * empty optional will be returned, in which case some default behaviour should happen, e.g. prompting.
     *
     * @param  arguments the command line arguments
     * @param  clock     the clock to find the current time
     * @return           the job query, if one was set on the command line
     */
    static Optional<JobQuery> forIngest(CommandArguments arguments, Clock clock) {
        List<JobQuery> queries = JobQueryType.INGEST_OPTIONS.stream()
                .flatMap(type -> type.parser().read(arguments, clock).stream())
                .toList();
        if (queries.size() > 1) {
            throw new CommandArgumentsException(
                    "Cannot combine query types. Options have been set for the following types: " +
                            queries.stream().map(JobQuery::getType).map(JobQueryType::name).collect(joining(", ")));
        }
        return queries.stream().findFirst();
    }

    /**
     * Creates a query for jobs based on parameters. To allow the PROMPT query type,
     * use {@link #fromParametersOrPrompt}.
     *
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  clock           the clock to find the current time
     * @return                 the query
     */
    static JobQuery from(JobQueryType queryType, String queryParameters, Clock clock) {
        return queryType.parser().read(queryParameters, clock);
    }

    /**
     * Creates a query for jobs based on parameters. Takes input from the console for the PROMPT query type.
     *
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  clock           the clock to find the current time
     * @param  input           the console to read from for the PROMPT query type
     * @return                 the query
     */
    static JobQuery fromParametersOrPrompt(
            JobQueryType queryType, String queryParameters, Clock clock, ConsoleInput input) {
        return fromParametersOrPrompt(queryType, queryParameters, clock, input, Map.of());
    }

    /**
     * Creates a query for jobs based on parameters. Takes input from the console for the PROMPT query type.
     *
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  clock           the clock to find the current time
     * @param  input           the console to read from for the PROMPT query type
     * @param  extraQueryTypes the
     * @return                 the query
     */
    static JobQuery fromParametersOrPrompt(
            JobQueryType queryType, String queryParameters, Clock clock, ConsoleInput input,
            Map<String, JobQuery> extraQueryTypes) {
        if (queryType == JobQueryType.PROMPT) {
            return JobQueryPrompt.from(clock, input, extraQueryTypes);
        }
        return from(queryType, queryParameters, clock);
    }
}
