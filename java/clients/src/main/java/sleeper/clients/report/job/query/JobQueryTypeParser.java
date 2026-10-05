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

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A parser to read a query of a certain type against a job tracker.
 */
public class JobQueryTypeParser {

    private final List<CommandOption> options;
    private final JobQueryType type;
    private final ByParameters byParameters;
    private final ByArguments byArguments;

    public JobQueryTypeParser(CommandOption option, JobQueryType type, Supplier<JobQuery> constructor) {
        this(option, type, (params, time) -> constructor.get(), (args, time) -> constructor.get());
    }

    public JobQueryTypeParser(CommandOption option, JobQueryType type, ByParameters byParameters, ByArguments byArguments) {
        this(List.of(option), type, byParameters, byArguments);
    }

    public JobQueryTypeParser(List<CommandOption> options, JobQueryType type, ByParameters byParameters, ByArguments byArguments) {
        this.options = options;
        this.type = type;
        this.byParameters = byParameters;
        this.byArguments = byArguments;
    }

    /**
     * Parses a job tracker query from query parameters.
     *
     * @param  foundType       the job query type
     * @param  queryParameters the parameters
     * @param  clock           a clock to get the current time
     * @return                 the query, if this parser supports the given type
     */
    public JobQuery read(String queryParameters, Clock clock) {
        if (type.isParametersRequired() && queryParameters == null) {
            throw new IllegalArgumentException("No parameters provided for query type " + type);
        }
        return byParameters.read(queryParameters, clock);
    }

    /**
     * Parses a job tracker query from command line arguments.
     *
     * @param  arguments the arguments
     * @param  clock     a clock to get the current time
     * @return           the query, if an argument supported by this parser is set
     */
    public Optional<JobQuery> read(CommandArguments arguments, Clock clock) {
        if (options.stream().anyMatch(arguments::isSet)) {
            try {
                return Optional.of(byArguments.read(arguments, clock));
            } catch (RuntimeException e) {
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
         * @param  clock           a clock to get the current time
         * @return                 the query
         */
        JobQuery read(String queryParameters, Clock clock);
    }

    /**
     * A parser method that takes command line arguments.
     */
    public interface ByArguments {

        /**
         * Parses arguments into a jobs query.
         *
         * @param  arguments the arguments
         * @param  clock     a clock to get the current time
         * @return           the query
         */
        JobQuery read(CommandArguments arguments, Clock clock);
    }

}
