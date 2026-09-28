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

/**
 * An option that may be set on the command line. Used with {@link CommandArguments}.
 */
public interface CommandOption {

    /**
     * Creates an option that must be set as a long flag, with no arguments.
     *
     * @param  name the name of the option to use like "--name"
     * @return      the option
     */
    public static CommandOption longFlag(String name) {
        return new CommandOptionImpl(name, null, NumArgs.NONE);
    }

    /**
     * Creates an option that must be set like "--name value". The next argument after the option will be taken as the
     * value for the option.
     *
     * @param  name the name of the option to use like "--name"
     * @return      the option
     */
    public static CommandOption longOption(String name) {
        return new CommandOptionImpl(name, null, NumArgs.ONE);
    }

    /**
     * Creates an option that can be set as a short or long flag, with no arguments.
     *
     * @param  character the character to use like "-c"
     * @param  name      the name of the option to use like "--name"
     * @return           the option
     */
    public static CommandOption shortFlag(char character, String name) {
        return new CommandOptionImpl(name, character, NumArgs.NONE);
    }

    /**
     * Creates an option that can be set like "--name value", "-c value" or "-cvalue".
     *
     * @param  character the character to use like "-c"
     * @param  name      the name of the option to use like "--name"
     * @return           the option
     */
    public static CommandOption shortOption(char character, String name) {
        return new CommandOptionImpl(name, character, NumArgs.ONE);
    }

    /**
     * Returns the long name, where the option can be set with `--name`.
     *
     * @return the long name
     */
    String longName();

    /**
     * Returns the short name, where the option can be set with `-n`, or null if it cannot.
     *
     * @return the short name, or null if there is none
     */
    Character shortName();

    /**
     * Returns the number of arguments that the option can take.
     *
     * @return the number of arguments
     */
    NumArgs numArgs();

    /**
     * Returns true if this is a flag that takes no arguments.
     *
     * @return whether this is a flag or not
     */
    default boolean isFlag() {
        return numArgs() == NumArgs.NONE;
    }

    /**
     * How many arguments a command line option can take.
     */
    public enum NumArgs {
        NONE, ONE
    }
}
