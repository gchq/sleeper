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

package sleeper.clients.util.command;

import java.io.IOException;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

@FunctionalInterface
public interface CommandPipelineRunner extends CommandRunner {
    CommandPipelineResult run(CommandPipeline pipeline) throws IOException, InterruptedException;

    @Override
    default int run(String... command) throws IOException, InterruptedException {
        return run(pipeline(command(command))).getLastExitCode();
    }

    default void runOrThrow(CommandPipeline pipeline) throws IOException, InterruptedException {
        int exitCode = run(pipeline).getLastExitCode();
        if (exitCode != 0) {
            throw new CommandFailedException(pipeline, exitCode);
        }
    }

    default void runOrThrow(String... command) throws IOException, InterruptedException {
        runOrThrow(pipeline(command(command)));
    }
}
