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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.util.NumberFormatUtils.countWithCommas;

public class ClientUtils {
    public static final Logger LOGGER = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static Optional<String> optionalArgument(String[] args, int index) {
        if (args.length > index) {
            return Optional.of(args[index]);
        } else {
            return Optional.empty();
        }
    }

    private static final long K_COUNT = 1_000;
    private static final long M_COUNT = 1_000_000;
    private static final long G_COUNT = 1_000_000_000;
    private static final long T_COUNT = 1_000_000_000_000L;

    public static String abbreviatedRecordCount(long records) {
        if (records < K_COUNT) {
            return "" + records;
        } else if (records < M_COUNT) {
            return Math.round((double) records / K_COUNT) + "K (" + countWithCommas(records) + ")";
        } else if (records < G_COUNT) {
            return Math.round((double) records / M_COUNT) + "M (" + countWithCommas(records) + ")";
        } else if (records < T_COUNT) {
            return Math.round((double) records / G_COUNT) + "G (" + countWithCommas(records) + ")";
        } else {
            return countWithCommas(Math.round((double) records / T_COUNT)) + "T (" + countWithCommas(records) + ")";
        }
    }

    public static void clearDirectory(Path tempDir) throws IOException {
        try (Stream<Path> paths = Files.walk(tempDir)) {
            Stream<Path> nestedPaths = paths.skip(1).sorted(Comparator.reverseOrder());
            for (Path path : (Iterable<Path>) nestedPaths::iterator) {
                Files.delete(path);
            }
        }
    }

    public static int runCommandInheritIO(String... command) throws IOException, InterruptedException {
        return runCommandInheritIO(pipeline(command(command))).getLastExitCode();
    }

    public static CommandPipelineResult runCommandInheritIO(CommandPipeline pipeline) throws IOException, InterruptedException {
        LOGGER.info("Running command: {}", pipeline);
        List<Process> processes = pipeline.startProcessesInheritIO();
        CommandPipelineResult result = waitFor(processes);
        LOGGER.info("Exit code: {}", result);
        return result;
    }

    public static CommandPipelineResult waitFor(List<Process> processes) throws InterruptedException {
        int size = processes.size();
        int[] exitCodes = new int[size];
        for (int i = 0; i < size; i++) {
            exitCodes[i] = processes.get(i).waitFor();
        }
        return new CommandPipelineResult(exitCodes);
    }
}
