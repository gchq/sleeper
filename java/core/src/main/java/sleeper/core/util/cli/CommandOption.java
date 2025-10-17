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

import java.util.Objects;

/**
 * An option that may be set on the command line. Used with {@link CommandArguments}.
 *
 * @param longName  the name for when the option is set like "--longName"
 * @param shortName the character for when the option is set like "-a"
 * @param numArgs   the number of arguments that must be passed after this option
 */
public record CommandOption(String longName, Character shortName, NumArgs numArgs) {

    public CommandOption {
        Objects.requireNonNull(longName, "longName must not be null");
        Objects.requireNonNull(numArgs, "numArgs must not be null");
    }

    /**
     * Creates an option that must be set as a long flag, with no arguments.
     *
     * @param  name the name of the option to use like "--name"
     * @return      the option
     */
    public static CommandOption longFlag(String name) {
        return longOption(name, NumArgs.NONE);
    }

    /**
     * Creates an option that must be set as a long flag, with no arguments.
     *
     * @param  name the name of the option to use like "--name"
     * @return      the option
     */
    public static CommandOption longOption(String name, NumArgs numArgs) {
        return new CommandOption(name, null, numArgs);
    }

    /**
     * Creates an option that can be set as a short or long flag, with no arguments.
     *
     * @param  character the character to use like "-c"
     * @param  name      the name of the option to use like "--name"
     * @return           the option
     */
    public static CommandOption shortFlag(char character, String name) {
        return shortOption(character, name, NumArgs.NONE);
    }

    /**
     * Creates an option that can be set as a short or long flag, with no arguments.
     *
     * @param  character the character to use like "-c"
     * @param  name      the name of the option to use like "--name"
     * @return           the option
     */
    public static CommandOption shortOption(char character, String name, NumArgs numArgs) {
        return new CommandOption(name, character, numArgs);
    }

    /**
     * How many arguments a command line option can take.
     */
    public enum NumArgs {
        NONE, ONE
    }
}
