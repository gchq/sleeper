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
package sleeper.core.util.cli;

import java.util.Objects;
import java.util.Optional;

/**
 * An option that may be set on the command line. Used with {@link CommandArguments}.
 */
public class CommandOption {

    private final String longName;
    private final Character shortName;
    private final NumArgs numArgs;
    private final String helpText;

    private CommandOption(Builder builder) {
        longName = Objects.requireNonNull(builder.longName, "longName must not be null");
        shortName = builder.shortName;
        numArgs = Objects.requireNonNull(builder.numArgs, "numArgs must not be null");
        helpText = builder.helpText;
    }

    public static Builder withLongName(String longName) {
        return new Builder().longName(longName);
    }

    /**
     * Creates an option that must be set as a long flag, with no arguments.
     *
     * @param  name the name of the option to use like "--name"
     * @return      the option
     */
    public static CommandOption longFlag(String name) {
        return withLongName(name).build();
    }

    /**
     * Creates an option that must be set like "--name value". The next argument after the option will be taken as the
     * value for the option.
     *
     * @param  name the name of the option to use like "--name"
     * @return      the option
     */
    public static CommandOption longOption(String name) {
        return withLongName(name).numArgs(NumArgs.ONE).build();
    }

    /**
     * Creates an option that can be set as a short or long flag, with no arguments.
     *
     * @param  character the character to use like "-c"
     * @param  name      the name of the option to use like "--name"
     * @return           the option
     */
    public static CommandOption shortFlag(char character, String name) {
        return withLongName(name).shortName(character).build();
    }

    /**
     * Creates an option that can be set like "--name value", "-c value" or "-cvalue".
     *
     * @param  character the character to use like "-c"
     * @param  name      the name of the option to use like "--name"
     * @return           the option
     */
    public static CommandOption shortOption(char character, String name) {
        return withLongName(name).shortName(character).numArgs(NumArgs.ONE).build();
    }

    /**
     * Returns the long name, where the option can be set with `--name`.
     *
     * @return the long name
     */
    public String longName() {
        return longName;
    }

    /**
     * Returns the short name, where the option can be set with `-n`, or null if it cannot.
     *
     * @return the short name, or null if there is none
     */
    public Character shortName() {
        return shortName;
    }

    /**
     * Returns the number of arguments that the option can take.
     *
     * @return the number of arguments
     */
    public NumArgs numArgs() {
        return numArgs;
    }

    /**
     * Returns true if this is a flag that takes no arguments.
     *
     * @return whether this is a flag or not
     */
    public boolean isFlag() {
        return numArgs == NumArgs.NONE;
    }

    /**
     * Returns the help text, if there is any.
     *
     * @return the help text
     */
    public Optional<String> helpText() {
        return Optional.ofNullable(helpText);
    }

    /**
     * How many arguments a command line option can take.
     */
    public enum NumArgs {
        NONE, ONE
    }

    public static class Builder {

        private String longName;
        private Character shortName;
        private NumArgs numArgs = NumArgs.NONE;
        private String helpText;

        private Builder() {
        }

        public Builder longName(String longName) {
            this.longName = longName;
            return this;
        }

        public Builder shortName(Character shortName) {
            this.shortName = shortName;
            return this;
        }

        public Builder numArgs(NumArgs numArgs) {
            this.numArgs = numArgs;
            return this;
        }

        public Builder helpText(String helpText) {
            this.helpText = helpText;
            return this;
        }

        public CommandOption build() {
            return new CommandOption(this);
        }

    }
}
