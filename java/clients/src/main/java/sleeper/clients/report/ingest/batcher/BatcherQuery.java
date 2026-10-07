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

package sleeper.clients.report.ingest.batcher;

import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * A query to generate a report based on files in the ingest batcher store. Different types of query can include files
 * based on their status.
 */
public enum BatcherQuery {
    ALL(option("all", 'a', "Reports on all files, whether waiting to be batched or already in jobs."),
            IngestBatcherStore::getAllFilesNewestFirst),
    PENDING(option("pending", 'p', "Reports on pending files, which have not yet been added to a job."),
            IngestBatcherStore::getPendingFilesOldestFirst);

    private final CommandOption option;
    private final Function<IngestBatcherStore, List<IngestBatcherTrackedFile>> runner;

    BatcherQuery(CommandOption option, Function<IngestBatcherStore, List<IngestBatcherTrackedFile>> runner) {
        this.option = option;
        this.runner = runner;
    }

    /**
     * Retrieves file tracking information from the store that matches this query.
     *
     * @param  store the ingest batcher store
     * @return       the file tracking information
     */
    public List<IngestBatcherTrackedFile> run(IngestBatcherStore store) {
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
        return Stream.of(values()).map(BatcherQuery::option).toList();
    }

    /**
     * Reads the query type set on the command line. If none is set, an empty optional will be returned, in which case
     * some default behaviour should happen, e.g. prompting.
     *
     * @param  arguments the command line arguments
     * @return           the query, if exactly one type is set
     */
    public static Optional<BatcherQuery> readOneOf(CommandArguments arguments) {
        List<BatcherQuery> queries = Stream.of(values())
                .filter(query -> arguments.isSet(query.option()))
                .toList();
        if (queries.size() > 1) {
            throw new CommandArgumentsException(
                    "Cannot combine query types. Options have been set for the following types: " +
                            queries.stream().map(BatcherQuery::name).collect(joining(", ")));
        }
        return queries.stream().findFirst();
    }

    private static CommandOption option(String longName, char shortName, String helpText) {
        return CommandOption.withLongName(longName).shortName(shortName).helpText(helpText).build();
    }
}
