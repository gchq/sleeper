/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.clients.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// SpotBugs complains because exceptions are serializable, but we don't need to serialise this class
@SuppressFBWarnings("SE_BAD_FIELD")
public class CommandFailedException extends RuntimeException {

    private final CommandPipeline command;
    private final int exitCode;

    public CommandFailedException(CommandPipeline command, int exitCode) {
        super("Command failed with exit code " + exitCode + ": " + command);
        this.command = command;
        this.exitCode = exitCode;
    }

    public CommandPipeline getCommand() {
        return command;
    }

    public int getExitCode() {
        return exitCode;
    }
}
