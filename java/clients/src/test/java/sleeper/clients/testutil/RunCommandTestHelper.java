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
import sleeper.clients.util.RunCommand;
import sleeper.clients.util.RunCommandPipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RunCommandTestHelper {
    private RunCommandTestHelper() {
    }

    public static List<CommandPipeline> pipelinesRunOn(PipelineInvoker invoker) throws IOException, InterruptedException {
        return pipelinesRunOn(invoker, pipeline -> 0);
    }

    public static List<CommandPipeline> pipelinesRunOn(
            PipelineInvoker invoker, RunCommandPipeline runner)
            throws IOException, InterruptedException {
        List<CommandPipeline> pipelines = new ArrayList<>();
        RunCommandPipeline runCommand = (pipeline) -> {
            pipelines.add(pipeline);
            return runner.run(pipeline);
        };
        invoker.run(runCommand);
        return pipelines;
    }

    public static RunCommandPipeline returningExitCode(int exitCode) {
        return pipeline -> exitCode;
    }

    public static RunCommandPipeline returningExitCodeForCommand(int exitCode, CommandPipeline command) {
        return foundCommand -> foundCommand.equals(command) ? exitCode : 0;
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
        void run(RunCommand runCommand) throws IOException, InterruptedException;
    }

    public interface PipelineInvoker {
        void run(RunCommandPipeline runCommand) throws IOException, InterruptedException;
    }

    private static Command singleCommandInPipeline(CommandPipeline pipeline) {
        List<Command> commands = pipeline.getCommands();
        if (commands.size() != 1) {
            throw new IllegalStateException("Exactly one command expected, found: " + commands);
        }
        return commands.get(0);
    }

}
