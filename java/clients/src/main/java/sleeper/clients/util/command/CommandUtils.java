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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.ClientUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class CommandUtils {
    public static final Logger LOGGER = LoggerFactory.getLogger(CommandUtils.class);

    private CommandUtils() {
    }

    public static int runCommandLogOutput(String... commands) throws IOException, InterruptedException {
        return runCommandLogOutput(pipeline(command(commands))).getLastExitCode();
    }

    public static CommandPipelineResult runCommandLogOutput(CommandPipeline pipeline) throws IOException, InterruptedException {
        LOGGER.info("Running command: {}", pipeline);
        List<Process> processes = pipeline.startProcesses();
        CompletableFuture<Void> logOutput = CompletableFuture.allOf(processes.stream()
                .map(CommandUtils::logOutput)
                .toArray(CompletableFuture[]::new));
        CommandPipelineResult result = ClientUtils.waitFor(processes);
        logOutput.join();
        LOGGER.info("Exit code: {}", result);
        return result;
    }

    private static CompletableFuture<Void> logOutput(Process process) {
        return CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> logTo(process.getInputStream(), LOGGER::info)),
                CompletableFuture.runAsync(() -> logTo(process.getErrorStream(), LOGGER::error)));
    }

    private static void logTo(InputStream stream, Consumer<String> logLine) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            String line = reader.readLine();
            while (line != null) {
                logLine.accept(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
