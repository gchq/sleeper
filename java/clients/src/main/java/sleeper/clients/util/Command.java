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

package sleeper.clients.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.util.Objects.requireNonNull;

public class Command {

    private final Map<String, String> envVars;
    private final String[] command;

    private Command(Map<String, String> envVars, String[] command) {
        this.envVars = requireNonNull(envVars, "envVars must not be null");
        this.command = requireNonNull(command, "command must not be null");
    }

    public static Command envAndCommand(Map<String, String> envVars, String... command) {
        return new Command(envVars, command);
    }

    public static Command command(String... command) {
        return new Command(Map.of(), command);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Command)) {
            return false;
        }
        Command other = (Command) obj;
        return Objects.equals(envVars, other.envVars) && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(command);
        result = prime * result + Objects.hash(envVars);
        return result;
    }

    @Override
    public String toString() {
        if (envVars.isEmpty()) {
            return Arrays.toString(command);
        } else {
            return Arrays.toString(command) + envVars.toString();
        }
    }

    public String[] toArray() {
        return command;
    }

    public ProcessBuilder toProcessBuilder() {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().putAll(envVars);
        return builder;
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
