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

package sleeper.clients.report.query;

import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A query to retrieve the status of queries held in the query tracker, to generate a report.
 */
public enum QueryTrackerQuery {
    ALL(option("all", 'a', "Reports on all queries."),
            QueryTrackerStore::getAllQueries),
    QUEUED(option("queued", 'q', "Reports on queued queries."),
            store -> store.getQueriesWithState(QueryState.QUEUED)),
    IN_PROGRESS(option("in-progress", 'i', "Reports on queries in progress."),
            store -> store.getQueriesWithState(QueryState.IN_PROGRESS)),
    COMPLETED(option("completed", 'c', "Reports on completed queries."),
            store -> store.getQueriesWithState(QueryState.COMPLETED)),
    FAILED(option("failed", 'f', "Reports on failed and partially failed queries."),
            QueryTrackerStore::getFailedQueries);

    private final CommandOption option;
    private final Function<QueryTrackerStore, List<TrackedQuery>> runner;

    QueryTrackerQuery(CommandOption option, Function<QueryTrackerStore, List<TrackedQuery>> runner) {
        this.option = option;
        this.runner = runner;
    }

    /**
     * Retrieves the data for the report.
     *
     * @param  store the tracker store
     * @return       the status of queries covered by this query
     */
    public List<TrackedQuery> run(QueryTrackerStore store) {
        return runner.apply(store);
    }

    /**
     * Retrieves the command line option that selects this query type.
     *
     * @return the option
     */
    public CommandOption option() {
        return option;
    }

    /**
     * Retrieves the command line options for all query types.
     *
     * @return the options
     */
    public static List<CommandOption> options() {
        return Stream.of(values()).map(QueryTrackerQuery::option).toList();
    }

    /**
     * Reads the query type set on the command line. If none is set, an empty optional will be returned, in which case
     * some default behaviour should happen, e.g. prompting.
     *
     * @param  arguments the command line arguments
     * @return           the query, if exactly one type is set
     */
    public static Optional<QueryTrackerQuery> readOneOf(CommandArguments arguments) {
        List<QueryTrackerQuery> queries = Stream.of(values())
                .filter(query -> arguments.isSet(query.option()))
                .toList();
        if (queries.size() > 1) {
            throw new CommandArgumentsException(
                    "Cannot combine query types. Options have been set for the following types: " +
                            queries.stream().map(QueryTrackerQuery::name).collect(joining(", ")));
        }
        return queries.stream().findFirst();
    }

    private static CommandOption option(String longName, char shortName, String helpText) {
        return CommandOption.withLongName(longName).shortName(shortName).helpText(helpText).build();
    }
}
