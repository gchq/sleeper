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
package sleeper.core.util.cli;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * A model of arguments and options for the command line. Used with {@link CommandArguments}.
 */
public class CommandLineUsage {

    private final List<PositionalArgument> positionalArguments;
    private final String helpSummary;
    private final List<CommandOption> options = new ArrayList<>(List.of(CommandOption.longFlag("help")));
    private final Map<String, CommandOption> optionByLongName;
    private final Map<Character, CommandOption> optionByShortName;
    private final boolean passThroughExtraArguments;

    private CommandLineUsage(Builder builder) {
        positionalArguments = builder.positionalArguments();
        helpSummary = builder.helpSummary;
        Optional.ofNullable(builder.options).ifPresent(options::addAll);
        optionByLongName = options.stream().collect(toMap(CommandOption::longName, Function.identity()));
        optionByShortName = options.stream().filter(option -> option.shortName() != null).collect(toMap(CommandOption::shortName, Function.identity()));
        passThroughExtraArguments = builder.passThroughExtraArguments;
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
        return positionalArguments.get(index).name();
    }

    /**
     * Creates an exception with the usage message.
     *
     * @return an exception
     */
    public IllegalArgumentException createUsageException() {
        return new IllegalArgumentException(createUsageMessage());
    }

    public boolean isPassThroughExtraArguments() {
        return passThroughExtraArguments;
    }

    /**
     * Creates a usage message to explain how to call from the command line.
     *
     * @return the message
     */
    public String createUsageMessage() {
        List<String> parts = new ArrayList<>();
        List<String> positionalNames = positionalArguments.stream()
                .filter(PositionalArgument::showUsage)
                .map(PositionalArgument::name)
                .toList();
        if (!positionalNames.isEmpty()) {
            parts.add("Usage: " +
                    positionalNames.stream()
                            .map(name -> "<" + name + ">")
                            .collect(joining(" ")));
        }
        parts.add("Available options: " + options.stream()
                .map(CommandOption::longName)
                .map(name -> "--" + name)
                .collect(joining(", ")));
        return String.join("\n", parts);
    }

    /**
     * Creates help text to explain in detail how to call from the command line.
     *
     * @return the help text
     */
    public String createHelpText() {
        List<String> parts = new ArrayList<>();
        parts.add(createUsageMessage());
        if (helpSummary != null) {
            parts.add(helpSummary);
        }
        return String.join("\n\n", parts);
    }

    /**
     * A builder to create command line usage information.
     */
    public static class Builder {
        private List<String> positionalArguments;
        private List<String> systemArguments;
        private String helpSummary;
        private List<CommandOption> options;
        private boolean passThroughExtraArguments;

        private Builder() {
        }

        /**
         * Sets the names of the positional arguments. These are mandatory arguments which are always supplied in order.
         * The order you specify the names in here determines the order they must be passed on the command line. The
         * names set here are used in the usage message, and to retrieve the values after the arguments are parsed.
         *
         * @param  positionalArguments the names
         * @return                     this builder
         */
        public Builder positionalArguments(List<String> positionalArguments) {
            this.positionalArguments = positionalArguments;
            return this;
        }

        /**
         * Sets which arguments are system arguments. These arguments will be passed by the system instead of the user,
         * and will not appear in the usage message. If you use {@link #positionalArguments(List)}, the system arguments
         * must be included there to set where they will be passed in order.
         *
         * @param  systemArguments the names of the system arguments
         * @return                 this builder
         */
        public Builder systemArguments(List<String> systemArguments) {
            this.systemArguments = systemArguments;
            return this;
        }

        /**
         * Sets a summary of the command line tool. This will be displayed in the help text after basic usage
         * information but before further details.
         *
         * @param  helpSummary the summary
         * @return             this builder
         */
        public Builder helpSummary(String helpSummary) {
            this.helpSummary = helpSummary;
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

        /**
         * Sets whether to allow extra arguments to be given after all other arguments, to be passed through to
         * some other command.
         *
         * @param  passThroughExtraArguments true if pass through arguments are allowed, false otherwise
         * @return                           this builder
         */
        public Builder passThroughExtraArguments(boolean passThroughExtraArguments) {
            this.passThroughExtraArguments = passThroughExtraArguments;
            return this;
        }

        public CommandLineUsage build() {
            return new CommandLineUsage(this);
        }

        private List<PositionalArgument> positionalArguments() {
            List<String> positionalNames = Optional.ofNullable(positionalArguments).orElseGet(List::of);
            List<String> allSystemNames = Optional.ofNullable(systemArguments).orElseGet(List::of);
            if (positionalNames.isEmpty()) {
                return allSystemNames.stream().map(PositionalArgument::systemArgument).toList();
            } else {
                Set<String> systemNames = new HashSet<>(allSystemNames);
                List<PositionalArgument> arguments = positionalNames.stream().map(name -> {
                    if (systemNames.remove(name)) {
                        return PositionalArgument.systemArgument(name);
                    } else {
                        return PositionalArgument.create(name);
                    }
                }).toList();
                if (!systemNames.isEmpty()) {
                    throw new IllegalArgumentException("System arguments should be included as positional arguments: " + systemNames);
                }
                return arguments;
            }
        }

    }

}
