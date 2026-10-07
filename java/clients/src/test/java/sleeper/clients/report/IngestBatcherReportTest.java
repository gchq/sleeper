/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.report.IngestBatcherReport.Arguments;
import sleeper.clients.report.ingest.batcher.BatcherQuery;
import sleeper.clients.report.ingest.batcher.JsonIngestBatcherReporter;
import sleeper.clients.report.ingest.batcher.StandardIngestBatcherReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IngestBatcherReportTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            // When
            Arguments args = readArguments("my-instance", "--all");

            // Then
            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.reporter()).isInstanceOf(StandardIngestBatcherReporter.class);
        }

        @Test
        void shouldReadReportTypeJson() {
            // When
            Arguments args = readArguments("json-instance", "--format", "json", "--all");

            // Then
            assertThat(args.reporter()).isInstanceOf(JsonIngestBatcherReporter.class);
        }
    }

    @Nested
    @DisplayName("All files")
    class AllFiles {

        @Test
        void shouldQueryAllFiles() {
            assertThat(queryFromArguments("all-instance", "--all"))
                    .isEqualTo(BatcherQuery.ALL);
        }

        @Test
        void shouldQueryAllFilesWithShortFlag() {
            assertThat(queryFromArguments("all-instance", "-a"))
                    .isEqualTo(BatcherQuery.ALL);
        }
    }

    @Nested
    @DisplayName("Pending files")
    class PendingFiles {

        @Test
        void shouldQueryPendingFiles() {
            assertThat(queryFromArguments("pending-instance", "--pending"))
                    .isEqualTo(BatcherQuery.PENDING);
        }

        @Test
        void shouldQueryPendingFilesWithShortFlag() {
            assertThat(queryFromArguments("pending-instance", "-p"))
                    .isEqualTo(BatcherQuery.PENDING);
        }
    }

    @Nested
    @DisplayName("Prompt for query type")
    class Prompt {

        @Test
        void shouldPromptForQueryTypeWhenNoFlagSet() {
            assertThat(queryFromArgumentsWithInput("p\n", "prompt-instance"))
                    .isEqualTo(BatcherQuery.PENDING);
        }

        @Test
        void shouldPromptAgainWhenInvalidQueryTypeEntered() {
            assertThat(queryFromArgumentsWithInput("x\na\n", "prompt-instance"))
                    .isEqualTo(BatcherQuery.ALL);
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            assertThatThrownBy(() -> readArguments("my-instance", "--format", "BAD-REPORT", "--all"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Output format not supported: BAD-REPORT. Valid formats: JSON, STANDARD");
        }

        @Test
        void shouldRejectMultipleFlagsSet() {
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "--all", "--pending"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, PENDING");
        }

        @Test
        void shouldRejectMultipleFlagsSetAsCombinedShortFlags() {
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "-ap"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, PENDING");
        }

        @Test
        void shouldRejectMissingInstanceId() {
            assertThatThrownBy(() -> readArguments("--all"))
                    .isInstanceOf(CommandArgumentsException.class);
        }
    }

    private static BatcherQuery queryFromArguments(String... args) {
        return readArguments(args).query();
    }

    private static BatcherQuery queryFromArgumentsWithInput(String input, String... args) {
        return readArguments(consoleInputFrom(input), args).query();
    }

    private static Arguments readArguments(String... args) {
        return readArguments(consoleInputFrom(""), args);
    }

    private static Arguments readArguments(ConsoleInput input, String... args) {
        return IngestBatcherReport.readArguments(
                CommandArgumentReader.parse(IngestBatcherReport.USAGE, args), input);
    }

    private static ConsoleInput consoleInputFrom(String input) {
        return new ConsoleInput(null, new PrintStream(System.out),
                new Scanner(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))));
    }
}
