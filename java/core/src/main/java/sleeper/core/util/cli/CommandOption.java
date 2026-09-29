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
    private final String argsHelpText;

    private CommandOption(Builder builder) {
        longName = Objects.requireNonNull(builder.longName, "longName must not be null");
        shortName = builder.shortName;
        numArgs = Objects.requireNonNull(builder.numArgs, "numArgs must not be null");
        helpText = builder.helpText;
        argsHelpText = builder.argsHelpText;
        if (helpText != null && numArgs != NumArgs.NONE) {
            Objects.requireNonNull(argsHelpText, "argsHelpText must be set when helpText is set for an option with arguments");
        }
        if (argsHelpText != null) {
            Objects.requireNonNull(helpText, "helpText must be set when argsHelpText is set");
            if (numArgs == NumArgs.NONE) {
                throw new IllegalArgumentException("cannot set argsHelpText for an option taking no arguments");
            }
        }
    }

    /**
     * Creates a builder for a command option with a given long name, to be set like "--name". Defaults to a
     * flag with no arguments. Further functionality can be set on the builder.
     *
     * @param  longName the long name
     * @return          the builder
     */
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
     * Returns the short name, if the option can be set with a short name like `-n`.
     *
     * @return the short name, if the option has one
     */
    public Optional<Character> shortName() {
        return Optional.ofNullable(shortName);
    }

    /**
     * Returns the short name, where the option can be set with `-n`, or null if it cannot.
     *
     * @return the short name, or null if there is none
     */
    public Character shortNameOrNull() {
        return shortName;
    }

    /**
     * Returns whether the option can be set with a short name like `-n`.
     *
     * @return true if the option has a short name
     */
    public boolean hasShortName() {
        return shortName != null;
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
     * Returns the text to display the option's arguments in help text, if there is any. For example, {@code <value>}
     * will be shown as {@code --option <value>} for an option with long name {@code option}.
     *
     * @return the text to display the option's arguments in help text
     */
    public Optional<String> argsHelpText() {
        return Optional.ofNullable(argsHelpText);
    }

    /**
     * How many arguments a command line option can take.
     */
    public enum NumArgs {
        NONE, ONE
    }

    /**
     * A builder to create a command line option.
     */
    public static class Builder {

        private String longName;
        private Character shortName;
        private NumArgs numArgs = NumArgs.NONE;
        private String helpText;
        private String argsHelpText;

        private Builder() {
        }

        /**
         * Sets the long name to set the option like "--name".
         *
         * @param  longName the long name
         * @return          this builder, for method chaining
         */
        public Builder longName(String longName) {
            this.longName = longName;
            return this;
        }

        /**
         * Sets the short name to set the option like "-n".
         *
         * @param  shortName the short name
         * @return           this builder, for method chaining
         */
        public Builder shortName(Character shortName) {
            this.shortName = shortName;
            return this;
        }

        /**
         * Sets the number of arguments the option can take.
         *
         * @param  numArgs the number of arguments
         * @return         this builder, for method chaining
         */
        public Builder numArgs(NumArgs numArgs) {
            this.numArgs = numArgs;
            return this;
        }

        /**
         * Sets the help text for the option.
         *
         * @param  helpText the help text
         * @return          this builder, for method chaining
         */
        public Builder helpText(String helpText) {
            this.helpText = helpText;
            return this;
        }

        /**
         * Sets the help text for the arguments to this option. For example, longName {@code option} and argsHelpText
         * {@code <value>} will be displayed like {@code --option <value>}.
         *
         * @param  argsHelpText the help text
         * @return              this builder, for method chaining
         */
        public Builder argsHelpText(String argsHelpText) {
            this.argsHelpText = argsHelpText;
            return this;
        }

        public CommandOption build() {
            return new CommandOption(this);
        }

    }
}
