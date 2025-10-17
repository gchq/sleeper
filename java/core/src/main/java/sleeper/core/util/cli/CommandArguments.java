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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A utility to read command line arguments.
 */
public class CommandArguments {

    private final Map<String, String> argByName;
    private final Set<String> optionsSet;

    public CommandArguments(Builder builder) {
        argByName = builder.argByName;
        optionsSet = builder.optionsSet;
    }

    /**
     * Reads command line arguments and detects arguments and options. Exits if validation fails or if the help text is
     * requested.
     *
     * @param  usage     information about how arguments and options are specified
     * @param  arguments the arguments from the command line
     * @return           the parsed arguments
     */
    public static CommandArguments parseAndValidateOrExit(CommandLineUsage usage, String... arguments) {
        try {
            return CommandArgumentReader.parse(usage, arguments);
        } catch (RuntimeException e) {
            System.out.println(usage.createUsageMessage());
            System.exit(1);
            return null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Retrieves the value of a mandatory argument.
     *
     * @param  name the name of the argument
     * @return      the value
     */
    public String getString(String name) {
        return Objects.requireNonNull(argByName.get(name), "Argument was not set: " + name);
    }

    /**
     * Retrieves the value of an optional argument.
     *
     * @param  name the name of the argument
     * @return      the value, if it was set
     */
    public Optional<String> getOptionalString(String name) {
        return Optional.ofNullable(argByName.get(name));
    }

    /**
     * Retrieves the value of a mandatory integer argument.
     *
     * @param  name the name of the argument
     * @return      the value
     */
    public int getInteger(String name) {
        return Integer.parseInt(argByName.get(name));
    }

    /**
     * Retrieves the value of an optional integer argument.
     *
     * @param  name         the name of the argument
     * @param  defaultValue the value to return if the argument is not set
     * @return              the value
     */
    public int getIntegerOrDefault(String name, int defaultValue) {
        String string = argByName.get(name);
        if (string == null) {
            return defaultValue;
        } else {
            return Integer.parseInt(string);
        }
    }

    /**
     * Checks whether a flag was set.
     *
     * @param  name the name of the flag
     * @return      true if the flag was set
     */
    public boolean isFlagSet(String name) {
        return optionsSet.contains(name);
    }

    /**
     * A builder for this class.
     */
    public static class Builder {
        private Map<String, String> argByName = new LinkedHashMap<>();
        private Set<String> optionsSet = new LinkedHashSet<>();

        /**
         * Sets an argument.
         *
         * @param  name  the name of the argument
         * @param  value the value set on the command line
         * @return       this builder
         */
        public Builder argument(String name, String value) {
            argByName.put(name, value);
            return this;
        }

        /**
         * Sets a flag option.
         *
         * @param  option the option that was set as a flag
         * @return        this builder
         */
        public Builder flag(CommandOption option) {
            optionsSet.add(option.longName());
            if (option.shortName() != null) {
                optionsSet.add("" + option.shortName());
            }
            return this;
        }

        /**
         * Sets an option with an argument.
         *
         * @param  option   the option that was set
         * @param  argument the value of the argument
         * @return          this builder
         */
        public Builder option(CommandOption option, String argument) {
            return argument(option.longName(), argument)
                    .argument("" + option.shortName(), argument);
        }

        public CommandArguments build() {
            return new CommandArguments(this);
        }
    }

}
