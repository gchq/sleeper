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

import java.util.Arrays;

import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.util.Objects.requireNonNull;

public class Command {

    private final String[] command;

    public Command(String[] command) {
        this.command = requireNonNull(command, "command must not be null");
    }

    public static Command command(String... command) {
        return new Command(command);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Command command1 = (Command) o;
        return Arrays.equals(command, command1.command);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return Arrays.toString(command);
    }

    public String[] toArray() {
        return command;
    }

    public ProcessBuilder toProcessBuilder() {
        return new ProcessBuilder(command);
    }

    public ProcessBuilder toProcessBuilderInheritIO(int index, int pipelineSize) {
        ProcessBuilder builder = toProcessBuilder().redirectError(INHERIT);
        if (index == 0) {
            builder.redirectInput(INHERIT);
        }
        if (index == pipelineSize - 1) {
            builder.redirectOutput(INHERIT);
        }
        return builder;
    }
}
