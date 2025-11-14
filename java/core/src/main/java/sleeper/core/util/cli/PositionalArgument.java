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

/**
 * A positional argument that must be set on the command line. Used with {@link CommandArguments}.
 *
 * @param name      the name of the argument, used to retrieve it after parsing, and for use in a usage message,
 *                  e.g. if it's "arg" the message might be "Usage: command &lt;arg&gt;"
 * @param showUsage whether or not to include this argument in the usage message (usually only excluded if it is
 *                  to be passed by the system instead of the user)
 */
public record PositionalArgument(String name, boolean showUsage) {

    /**
     * Creates a positional argument where its name is the same as its description in the usage message.
     *
     * @param  name the name to retrieve the argument after parsing, and to be displayed in the usage message
     * @return      the argument
     */
    public static PositionalArgument create(String name) {
        return new PositionalArgument(name, true);
    }

    /**
     * Creates a positional argument that will be passed by the system instead of the user. Will be excluded from the
     * usage message.
     *
     * @param  name the name to retrieve the argument after parsing
     * @return      the argument
     */
    public static PositionalArgument systemArgument(String name) {
        return new PositionalArgument(name, false);
    }

}
