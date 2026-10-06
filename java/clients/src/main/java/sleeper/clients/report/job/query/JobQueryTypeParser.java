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

import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * A parser to read a job tracker query of a certain type. This can be used from command line arguments or from inside
 * a command line application where we may have prompted for parameters.
 */
public class JobQueryTypeParser {

    private final List<CommandOption> options;
    private final ByParameters byParameters;
    private final ByArguments byArguments;

    public JobQueryTypeParser(CommandOption option, Supplier<JobQuery> constructor) {
        this(option, (params, time) -> constructor.get(), (args, time) -> constructor.get());
    }

    public JobQueryTypeParser(CommandOption option, ByParameters byParameters, ByArguments byArguments) {
        this(List.of(option), byParameters, byArguments);
    }

    public JobQueryTypeParser(List<CommandOption> options, ByParameters byParameters, ByArguments byArguments) {
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.byParameters = Objects.requireNonNull(byParameters, "byParameters must not be null");
        this.byArguments = Objects.requireNonNull(byArguments, "byArguments must not be null");
    }

    /**
     * Parses a job tracker query from command line arguments with a restricted set of allowed query types. The list of
     * type options should not include prompting. If none is specified, an empty optional will be returned, in which
     * case some default behaviour should happen, e.g. prompting.
     *
     * @param  typeOptions  the allowed query types
     * @param  arguments    the arguments
     * @param  timeSupplier a supplier of the current time
     * @return              the query, if exactly one of the given types is set
     */
    public static Optional<JobQuery> readOneOfTypes(List<JobQueryType> typeOptions, CommandArguments arguments, Supplier<Instant> timeSupplier) {
        List<JobQuery> queries = typeOptions.stream()
                .flatMap(type -> type.parser().read(arguments, timeSupplier).stream())
                .toList();
        if (queries.size() > 1) {
            throw new CommandArgumentsException(
                    "Cannot combine query types. Options have been set for the following types: " +
                            queries.stream().map(JobQuery::getType).map(JobQueryType::name).collect(joining(", ")));
        }
        return queries.stream().findFirst();
    }

    /**
     * Parses a job tracker query from query parameters.
     *
     * @param  queryParameters the parameters
     * @param  timeSupplier    a supplier of the current time
     * @return                 the query, if this parser supports the given type
     */
    public JobQuery read(String queryParameters, Supplier<Instant> timeSupplier) {
        return byParameters.read(queryParameters, timeSupplier);
    }

    private Optional<JobQuery> read(CommandArguments arguments, Supplier<Instant> timeSupplier) {
        if (options.stream().anyMatch(arguments::isSet)) {
            try {
                return Optional.of(byArguments.read(arguments, timeSupplier));
            } catch (CommandArgumentsException e) {
                throw e;
            } catch (RuntimeException e) {
                // Since the scope of this catch is limited to just the parsing code,
                // we hope any internal parsing failures should already be wrapped with a useful message.
                throw new CommandArgumentsException(e);
            }
        } else {
            return Optional.empty();
        }
    }

    /**
     * Retrieves a list of command line options that trigger this query type.
     *
     * @return the options
     */
    public List<CommandOption> options() {
        return options;
    }

    /**
     * A parser method that takes a string for its parameters.
     */
    public interface ByParameters {

        /**
         * Parses query parameters into a jobs query.
         *
         * @param  queryParameters the parameters
         * @param  timeSupplier    a supplier of the current time
         * @return                 the query
         */
        JobQuery read(String queryParameters, Supplier<Instant> timeSupplier);
    }

    /**
     * A parser method that takes command line arguments.
     */
    public interface ByArguments {

        /**
         * Parses arguments into a jobs query.
         *
         * @param  arguments    the arguments
         * @param  timeSupplier a supplier of the current time
         * @return              the query
         */
        JobQuery read(CommandArguments arguments, Supplier<Instant> timeSupplier);
    }

}
