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
 * A validation failure for command line arguments. Any exception extending this class will be caught when parsing the
 * command line arguments, and the exception's message will be displayed to the user after a usage message. Exceptions
 * extending this should have a message designed to be displayed to the user alongside the usage message.
 *
 * @see CommandArguments#parseAndValidateOrExit
 */
public class CommandArgumentsException extends RuntimeException {

    public CommandArgumentsException(String message) {
        super(message);
    }

    public CommandArgumentsException(String message, Throwable cause) {
        super(message, cause);
    }
}
