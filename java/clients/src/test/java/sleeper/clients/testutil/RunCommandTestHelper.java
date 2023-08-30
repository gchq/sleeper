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

import sleeper.clients.util.RunCommand;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RunCommandTestHelper {
    private RunCommandTestHelper() {
    }

    public static List<Command> commandsRunOn(CommandInvoker invoker) throws IOException, InterruptedException {
        List<Command> commands = new ArrayList<>();
        RunCommand runCommand = (args) -> {
            commands.add(command(args));
            return 0;
        };
        invoker.run(runCommand);
        return commands;
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

}
