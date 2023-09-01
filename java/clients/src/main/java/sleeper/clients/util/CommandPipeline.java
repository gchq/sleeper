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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class CommandPipeline {

    private final List<Command> commands;

    public CommandPipeline(List<Command> commands) {
        this.commands = requireNonNull(commands, "commands must not be null");
    }

    public static CommandPipeline pipeline(Command... commands) {
        return new CommandPipeline(List.of(commands));
    }

    public List<Command> getCommands() {
        return commands;
    }

    public List<Process> startProcesses() throws IOException {
        if (commands.size() == 1) {
            return List.of(commands.get(0).toProcessBuilder().start());
        } else {
            return ProcessBuilder.startPipeline(commands.stream()
                    .map(Command::toProcessBuilder)
                    .collect(Collectors.toUnmodifiableList()));
        }
    }

    public List<Process> startProcessesInheritIO() throws IOException {
        int size = commands.size();
        if (size == 1) {
            return List.of(commands.get(0).toProcessBuilder().inheritIO().start());
        } else {
            List<ProcessBuilder> builders = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                builders.add(commands.get(i).toProcessBuilderInheritIO(i, size));
            }
            return ProcessBuilder.startPipeline(builders);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommandPipeline that = (CommandPipeline) o;
        return Objects.equals(commands, that.commands);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commands);
    }

    @Override
    public String toString() {
        if (commands.size() == 1) {
            return commands.get(0).toString();
        } else {
            return "CommandPipeline{" +
                    "commands=" + commands +
                    '}';
        }
    }
}
