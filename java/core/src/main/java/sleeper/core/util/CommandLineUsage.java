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

    private CommandLineUsage(Builder builder) {
        positionalArguments = builder.positionalArguments;
        optionByLongName = builder.options.stream().collect(toMap(CommandOption::longName, Function.identity()));
        optionByShortName = builder.options.stream().filter(option -> option.shortName() != null).collect(toMap(CommandOption::shortName, Function.identity()));
    }

    public static Builder builder() {
        return new Builder();
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
    public IllegalArgumentException createUsageException() {
        return new IllegalArgumentException(createUsageMessage());
    }

    /**
     * Creates a usage message to explain how to call from the command line.
     *
     * @return the message
     */
    public String createUsageMessage() {
        return "Usage: " +
                positionalArguments.stream()
                        .map(name -> "<" + name + ">")
                        .collect(joining(" "));
    }

    /**
     * A builder to create command line usage information.
     */
    public static class Builder {
        private List<String> positionalArguments;
        private List<CommandOption> options;

        private Builder() {
        }

        /**
         * Sets the names of the positional arguments. These are mandatory arguments which are always supplied in order.
         *
         * @param  positionalArguments the names
         * @return                     this builder
         */
        public Builder positionalArguments(List<String> positionalArguments) {
            this.positionalArguments = positionalArguments;
            return this;
        }

        /**
         * Sets the options that may be set in addition to positional arguments. This includes flags and arguments that
         * are set beginning like "--name" or "-n".
         *
         * @param  options the options
         * @return         this builder
         */
        public Builder options(List<CommandOption> options) {
            this.options = options;
            return this;
        }

        public CommandLineUsage build() {
            return new CommandLineUsage(this);
        }

    }

}
