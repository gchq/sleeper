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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A utility to read command line arguments.
 */
public class CommandArguments {

    private final Map<String, String> argByName;
    private final Map<String, Boolean> flags;
    private final List<String> passThroughArguments;

    public CommandArguments(Builder builder) {
        argByName = builder.argByName;
        flags = builder.flagByName;
        passThroughArguments = builder.passThroughArguments;
    }

    /**
     * Reads command line arguments and detects arguments and options. Exits if validation fails or if the help text is
     * requested. Takes a method to read the arguments, where validation failures will also be handled.
     *
     * @param  <T>           the type to read the arguments into
     * @param  usage         information about how arguments and options are specified
     * @param  arguments     the arguments from the command line
     * @param  readArguments the function to read the arguments
     * @return               the result from the function
     */
    public static <T> T parseAndValidateOrExit(CommandLineUsage usage, String[] arguments, Function<CommandArguments, T> readArguments) {
        try {
            return readArguments.apply(CommandArgumentReader.parse(usage, arguments).exitIfHelpRequested(usage));
        } catch (RuntimeException e) {
            exitWithFailure(usage, e);
            return null;
        }
    }

    /**
     * Reads command line arguments and detects arguments and options. Exits if validation fails or if the help text is
     * requested.
     *
     * @param  usage     information about how arguments and options are specified
     * @param  arguments the arguments from the command line
     * @return           the parsed arguments
     */
    public static CommandArguments parseAndValidateOrExit(CommandLineUsage usage, String[] arguments) {
        try {
            return CommandArgumentReader.parse(usage, arguments).exitIfHelpRequested(usage);
        } catch (RuntimeException e) {
            exitWithFailure(usage, e);
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
        return getOptionalString(name)
                .orElseThrow(() -> new CommandArgumentsException("Argument was not set: " + name));
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
        return readInteger(name, getString(name));
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
            return readInteger(name, string);
        }
    }

    /**
     * Checks whether a flag was set. Returns its value if it was set, otherwise returns false.
     *
     * @param  name the name of the flag
     * @return      the value if the flag was set, otherwise false
     */
    public boolean isFlagSet(String name) {
        return isFlagSetWithDefault(name, false);
    }

    /**
     * Checks whether a flag was set. Returns its value if it was set, otherwise returns a default.
     *
     * @param  name         the name of the flag
     * @param  defaultValue the default value
     * @return              the value if the flag was set, otherwise the default
     */
    public boolean isFlagSetWithDefault(String name, boolean defaultValue) {
        return flags.getOrDefault(name, defaultValue);
    }

    /**
     * Retrieves unrecognised arguments, if set to pass through unrecognised arguments.
     *
     * @return the pass-through arguments
     */
    public List<String> getPassthroughArguments() {
        return passThroughArguments;
    }

    private static int readInteger(String name, String string) {
        try {
            return Integer.parseInt(string);
        } catch (NumberFormatException e) {
            throw new CommandArgumentsException("Expected integer for argument \"" + name + "\", found \"" + string + "\"", e);
        }
    }

    private static void exitWithFailure(CommandLineUsage usage, RuntimeException e) {
        System.out.println(usage.createUsageMessage());
        if (e instanceof CommandArgumentsException) {
            System.out.println(e.getMessage());
        } else {
            e.printStackTrace();
        }
        System.exit(1);
    }

    private CommandArguments exitIfHelpRequested(CommandLineUsage usage) {
        if (isFlagSet("help")) {
            System.out.println(usage.createHelpText());
            System.exit(1);
        }
        return this;
    }

    /**
     * A builder for this class.
     */
    public static class Builder {
        private Map<String, String> argByName;
        private Map<String, Boolean> flagByName;
        private List<String> passThroughArguments;

        /**
         * Sets a map from argument name to value.
         *
         * @param  argByName the arguments with values
         * @return           this builder
         */
        public Builder argByName(Map<String, String> argByName) {
            this.argByName = argByName;
            return this;
        }

        /**
         * Sets a map from flag name to value.
         *
         * @param  flagByName the flags with values
         * @return            this builder
         */
        public Builder flagByName(Map<String, Boolean> flagByName) {
            this.flagByName = flagByName;
            return this;
        }

        /**
         * Sets the pass-through arguments, which are unrecognised arguments given after all other arguments.
         *
         * @param  passThroughArguments the pass-through arguments
         * @return                      this builder
         */
        public Builder passThroughArguments(List<String> passThroughArguments) {
            this.passThroughArguments = passThroughArguments;
            return this;
        }

        public CommandArguments build() {
            return new CommandArguments(this);
        }
    }

}
