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
 * Thrown when too many or too few positional arguments are provided. Note that the exception message will be displayed
 * to the user alongside a usage message that includes the list of expected arguments, in the order they are expected.
 *
 * @see CommandArguments#parseAndValidateOrExit
 */
public class WrongNumberOfArgumentsException extends CommandArgumentsException {

    public WrongNumberOfArgumentsException(int actualNumber, int expectedNumber) {
        super("Expected " + expectedNumber + " positional argument" + (expectedNumber == 1 ? "" : "s") + ", found " + actualNumber);
    }

}
