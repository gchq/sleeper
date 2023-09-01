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
package sleeper.clients.testutil;

import sleeper.clients.util.Command;
import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.CommandPipelineResult;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.CommandRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RunCommandTestHelper {
    private RunCommandTestHelper() {
    }

    public static List<CommandPipeline> pipelinesRunOn(PipelineInvoker invoker) throws IOException, InterruptedException {
        return pipelinesRunOn(invoker, pipeline -> new CommandPipelineResult(0));
    }

    public static List<CommandPipeline> pipelinesRunOn(
            PipelineInvoker invoker, CommandPipelineRunner runner)
            throws IOException, InterruptedException {
        List<CommandPipeline> pipelines = new ArrayList<>();
        CommandPipelineRunner runCommand = (pipeline) -> {
            pipelines.add(pipeline);
            return runner.run(pipeline);
        };
        invoker.run(runCommand);
        return pipelines;
    }

    public static CommandPipelineRunner returningExitCode(int exitCode) {
        return pipeline -> new CommandPipelineResult(exitCode);
    }

    public static CommandPipelineRunner returningExitCodeForCommand(int exitCode, CommandPipeline command) {
        return foundCommand -> new CommandPipelineResult(foundCommand.equals(command) ? exitCode : 0);
    }

    public static List<Command> commandsRunOn(CommandInvoker invoker) throws IOException, InterruptedException {
        return pipelinesRunOn(invoker::run).stream()
                .map(RunCommandTestHelper::singleCommandInPipeline)
                .collect(Collectors.toUnmodifiableList());
    }

    public static String[] commandRunOn(CommandInvoker invoker) throws IOException, InterruptedException {
        List<Command> commands = commandsRunOn(invoker);
        if (commands.size() != 1) {
            throw new IllegalStateException("Exactly one command expected, found: " + commands);
        }
        return commands.get(0).toArray();
    }

    public static Command command(String... command) {
        return new Command(command);
    }

    public interface CommandInvoker {
        void run(CommandRunner runCommand) throws IOException, InterruptedException;
    }

    public interface PipelineInvoker {
        void run(CommandPipelineRunner runCommand) throws IOException, InterruptedException;
    }

    private static Command singleCommandInPipeline(CommandPipeline pipeline) {
        List<Command> commands = pipeline.getCommands();
        if (commands.size() != 1) {
            throw new IllegalStateException("Exactly one command expected, found: " + commands);
        }
        return commands.get(0);
    }

}
