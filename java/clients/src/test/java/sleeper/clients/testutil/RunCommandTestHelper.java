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
package sleeper.clients.testutil;

import sleeper.clients.util.command.Command;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineResult;
import sleeper.clients.util.command.CommandPipelineRunner;

import java.util.List;

public class RunCommandTestHelper {
    private RunCommandTestHelper() {
    }

    public static CommandPipelineRunner returnExitCode(int exitCode) {
        return pipeline -> new CommandPipelineResult(exitCode);
    }

    public static CommandPipelineRunner returnExitCodeForCommand(int exitCode, CommandPipeline command) {
        return foundCommand -> new CommandPipelineResult(foundCommand.equals(command) ? exitCode : 0);
    }

    public static CommandPipelineRunner recordCommandsRun(List<CommandPipeline> commandsRun, CommandPipelineRunner runner) {
        return pipeline -> {
            commandsRun.add(pipeline);
            return runner.run(pipeline);
        };
    }

    public static CommandPipelineRunner recordCommandsRun(List<CommandPipeline> commandsRun) {
        return recordCommandsRun(commandsRun, returnExitCode(0));
    }

    public static String[] singleCommand(List<CommandPipeline> commands) {
        if (commands.size() != 1) {
            throw new IllegalStateException("Exactly one pipeline expected, found: " + commands);
        }
        return singleCommandInPipeline(commands.get(0)).toArray();
    }

    private static Command singleCommandInPipeline(CommandPipeline pipeline) {
        List<Command> commands = pipeline.getCommands();
        if (commands.size() != 1) {
            throw new IllegalStateException("Exactly one command expected, found: " + commands);
        }
        return commands.get(0);
    }

}
