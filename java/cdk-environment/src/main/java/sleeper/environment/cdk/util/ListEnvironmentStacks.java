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

package sleeper.environment.cdk.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.lang.ProcessBuilder.Redirect.INHERIT;

public interface ListEnvironmentStacks {

    List<String> stacksForEnvironment(String environmentId) throws IOException, InterruptedException;

    static ListEnvironmentStacks atWorkingDirectory() {
        return atDirectory(Path.of("").toAbsolutePath());
    }

    static ListEnvironmentStacks atDirectory(Path cdkDir) {
        return environmentId -> stacksForEnvironment(cdkDir, environmentId);
    }

    static List<String> stacksForEnvironment(Path cdkDir, String environmentId) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of(
                "cdk", "ls", "-c", String.format("instanceId=%s", environmentId)
        ));

        Process process = new ProcessBuilder(command.toArray(new String[0]))
                .redirectInput(INHERIT).redirectError(INHERIT).directory(cdkDir.toFile())
                .start();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            List<String> stacks = new ArrayList<>();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                stacks.add(line);
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new CdkFailedException(command, exitCode);
            }
            return stacks;
        }
    }
}
