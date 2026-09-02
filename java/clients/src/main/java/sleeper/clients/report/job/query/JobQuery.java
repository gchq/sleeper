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
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A query to generate a report based on jobs in a job tracker. Different types of query can include jobs based on their
 * status or other parameters.
 */
public interface JobQuery {

    /**
     * Retrieves compaction jobs matching this query.
     *
     * @param  tracker the job tracker
     * @return         the jobs
     */
    List<CompactionJobStatus> run(CompactionJobTracker tracker);

    /**
     * Retrieves ingest jobs matching this query.
     *
     * @param  tracker the job tracker
     * @return         the jobs
     */
    List<IngestJobStatus> run(IngestJobTracker tracker);

    /**
     * Retrieves the type of this query.
     *
     * @return the query type
     */
    Type getType();

    /**
     * Creates a query for jobs based on parameters. To allow the PROMPT query type,
     * use {@link #fromParametersOrPrompt}.
     *
     * @param  table           the Sleeper table to generate a report for
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  clock           the clock to find the current time
     * @return                 the query
     */
    static JobQuery from(TableStatus table, Type queryType, String queryParameters, Clock clock) {
        if (queryType.isParametersRequired() && queryParameters == null) {
            throw new IllegalArgumentException("No parameters provided for query type " + queryType);
        }
        switch (queryType) {
            case ALL:
                return new AllJobsQuery(table);
            case UNFINISHED:
                return new UnfinishedJobsQuery(table);
            case DETAILED:
                return DetailedJobsQuery.fromParameters(queryParameters);
            case RANGE:
                return RangeJobsQuery.fromParameters(table, queryParameters, clock);
            case REJECTED:
                return new RejectedJobsQuery();
            default:
                throw new IllegalArgumentException("Unexpected query type: " + queryType);
        }
    }

    /**
     * Creates a query for jobs based on parameters. Takes input from the console for the PROMPT query type.
     *
     * @param  table           the Sleeper table to generate a report for
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  clock           the clock to find the current time
     * @param  input           the console to read from for the PROMPT query type
     * @return                 the query
     */
    static JobQuery fromParametersOrPrompt(
            TableStatus table, Type queryType, String queryParameters, Clock clock, ConsoleInput input) {
        return fromParametersOrPrompt(table, queryType, queryParameters, clock, input, Map.of());
    }

    /**
     * Creates a query for jobs based on parameters. Takes input from the console for the PROMPT query type.
     *
     * @param  table           the Sleeper table to generate a report for
     * @param  queryType       the type of query to run
     * @param  queryParameters the parameters for the query, if required
     * @param  clock           the clock to find the current time
     * @param  input           the console to read from for the PROMPT query type
     * @param  extraQueryTypes the
     * @return                 the query
     */
    static JobQuery fromParametersOrPrompt(
            TableStatus table, Type queryType, String queryParameters, Clock clock,
            ConsoleInput input, Map<String, JobQuery> extraQueryTypes) {
        if (queryType == JobQuery.Type.PROMPT) {
            return JobQueryPrompt.from(table, clock, input, extraQueryTypes);
        }
        return from(table, queryType, queryParameters, clock);
    }

    /**
     * Method to determine what query type has been passed in as an argument.
     *
     * @param  args object containing all possible flags set
     * @return      query type that matches the flag set. Will default to ALL if none set.
     */
    static Type determineQueryType(CommandArguments args) {
        ArrayList<Type> typeList = new ArrayList<Type>();
        Stream.of(Type.values()).forEach(valueStr -> {
            String checkVal = valueStr.name().toLowerCase(Locale.ROOT);
            if (args.isFlagSet(checkVal) || args.getOptionalString(checkVal).isPresent()) {
                typeList.add(valueStr);
            }
        });

        if (typeList.size() > 1) {
            StringBuilder outStr = new StringBuilder();
            typeList.stream().forEach(type -> outStr.append(type.name() + ", "));
            throw new CommandArgumentsException("Too many query type flags are set, maximum of 1. Flags set: " + outStr.substring(0, outStr.length() - 2));
        } else if (typeList.size() == 0) {
            return Type.ALL;
        } else {
            return typeList.get(0);
        }
    }

    /**
     * The type of a query for jobs to include in a report.
     */
    enum Type {
        PROMPT,
        ALL,
        DETAILED,
        RANGE,
        UNFINISHED,
        REJECTED;

        public boolean isParametersRequired() {
            return this == DETAILED;
        }
    }
}
