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
package sleeper.core.util;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * A model of arguments and options for the command line.
 */
public class CommandLineUsage {

    private final List<String> positionalArguments;
    private final Map<String, CommandOption> optionByLongName;
    private final Map<Character, CommandOption> optionByShortName;

    private CommandLineUsage(List<String> positionalArguments, List<CommandOption> options) {
        this.positionalArguments = positionalArguments;
        optionByLongName = options.stream().collect(toMap(CommandOption::longName, Function.identity()));
        optionByShortName = options.stream().filter(option -> option.shortName() != null).collect(toMap(CommandOption::shortName, Function.identity()));
    }

    /**
     * Creates a model of command line usage.
     *
     * @param  positionalArguments the positional arguments
     * @param  options             the options
     * @return                     the model
     */
    public static CommandLineUsage positionalAndOptions(List<String> positionalArguments, List<CommandOption> options) {
        return new CommandLineUsage(positionalArguments, options);
    }

    /**
     * Retrieves an option by its long name.
     *
     * @param  name the long name
     * @return      the option, if it exists
     */
    public Optional<CommandOption> getLongOption(String name) {
        return Optional.ofNullable(optionByLongName.get(name));
    }

    /**
     * Retrieves an option by its short name.
     *
     * @param  name the short name
     * @return      the option, if it exists
     */
    public Optional<CommandOption> getShortOption(Character name) {
        return Optional.ofNullable(optionByShortName.get(name));
    }

    /**
     * Retrieves the number of positional arguments.
     *
     * @return the number
     */
    public int getNumPositionalArgs() {
        return positionalArguments.size();
    }

    /**
     * Retrieves the name of a positional argument.
     *
     * @param  index the index of the argument, starting from 0
     * @return       the name of the argument
     */
    public String getPositionalArgName(int index) {
        return positionalArguments.get(index);
    }

    /**
     * Creates an exception with the usage message.
     *
     * @return an exception
     */
    public IllegalArgumentException usageException() {
        return new IllegalArgumentException("Usage: " +
                positionalArguments.stream()
                        .map(name -> "<" + name + ">")
                        .collect(joining(" ")));
    }

}
