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

import sleeper.clients.report.QueryTrackerReport.Arguments;
import sleeper.clients.report.query.JsonQueryTrackerReporter;
import sleeper.clients.report.query.QueryTrackerQuery;
import sleeper.clients.report.query.StandardQueryTrackerReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class QueryTrackerReportTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            // When
            Arguments args = readArguments("my-instance", "--all");

            // Then
            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.reporter()).isInstanceOf(StandardQueryTrackerReporter.class);
        }

        @Test
        void shouldReadReportTypeJson() {
            // When
            Arguments args = readArguments("json-instance", "--format", "json", "--all");

            // Then
            assertThat(args.reporter()).isInstanceOf(JsonQueryTrackerReporter.class);
        }
    }

    @Nested
    @DisplayName("All queries")
    class AllQueries {

        @Test
        void shouldQueryAllQueries() {
            assertThat(queryFromArguments("all-instance", "--all"))
                    .isEqualTo(QueryTrackerQuery.ALL);
        }

        @Test
        void shouldQueryAllQueriesWithShortFlag() {
            assertThat(queryFromArguments("all-instance", "-a"))
                    .isEqualTo(QueryTrackerQuery.ALL);
        }
    }

    @Nested
    @DisplayName("Queued queries")
    class QueuedQueries {

        @Test
        void shouldQueryQueuedQueries() {
            assertThat(queryFromArguments("queued-instance", "--queued"))
                    .isEqualTo(QueryTrackerQuery.QUEUED);
        }

        @Test
        void shouldQueryQueuedQueriesWithShortFlag() {
            assertThat(queryFromArguments("queued-instance", "-q"))
                    .isEqualTo(QueryTrackerQuery.QUEUED);
        }
    }

    @Nested
    @DisplayName("In progress queries")
    class InProgressQueries {

        @Test
        void shouldQueryInProgressQueries() {
            assertThat(queryFromArguments("in-progress-instance", "--in-progress"))
                    .isEqualTo(QueryTrackerQuery.IN_PROGRESS);
        }

        @Test
        void shouldQueryInProgressQueriesWithShortFlag() {
            assertThat(queryFromArguments("in-progress-instance", "-p"))
                    .isEqualTo(QueryTrackerQuery.IN_PROGRESS);
        }
    }

    @Nested
    @DisplayName("Completed queries")
    class CompletedQueries {

        @Test
        void shouldQueryCompletedQueries() {
            assertThat(queryFromArguments("completed-instance", "--completed"))
                    .isEqualTo(QueryTrackerQuery.COMPLETED);
        }

        @Test
        void shouldQueryCompletedQueriesWithShortFlag() {
            assertThat(queryFromArguments("completed-instance", "-c"))
                    .isEqualTo(QueryTrackerQuery.COMPLETED);
        }
    }

    @Nested
    @DisplayName("Failed queries")
    class FailedQueries {

        @Test
        void shouldQueryFailedQueries() {
            assertThat(queryFromArguments("failed-instance", "--failed"))
                    .isEqualTo(QueryTrackerQuery.FAILED);
        }

        @Test
        void shouldQueryFailedQueriesWithShortFlag() {
            assertThat(queryFromArguments("failed-instance", "-f"))
                    .isEqualTo(QueryTrackerQuery.FAILED);
        }
    }

    @Nested
    @DisplayName("Single query by ID")
    class SingleQueryById {

        @Test
        void shouldQuerySingleQueryById() {
            // When
            Arguments args = readArguments("for-query-instance", "--query", "my-query");

            // Then
            assertThat(args.query()).isEqualTo(QueryTrackerQuery.FOR_QUERY);
            assertThat(args.queryId()).isEqualTo("my-query");
        }

        @Test
        void shouldQuerySingleQueryByIdWithShortOption() {
            // When
            Arguments args = readArguments("for-query-instance", "-i", "my-query");

            // Then
            assertThat(args.query()).isEqualTo(QueryTrackerQuery.FOR_QUERY);
            assertThat(args.queryId()).isEqualTo("my-query");
        }

        @Test
        void shouldNotSetQueryIdForOtherQueryTypes() {
            // When / Then
            assertThat(readArguments("all-instance", "--all").queryId()).isNull();
        }
    }

    @Nested
    @DisplayName("Prompt for query type")
    class Prompt {

        @Test
        void shouldPromptForQueryTypeWhenNoFlagSet() {
            assertThat(queryFromArgumentsWithInput("c\n", "prompt-instance"))
                    .isEqualTo(QueryTrackerQuery.COMPLETED);
        }

        @Test
        void shouldPromptForQueryIdWhenSingleQueryChosenInteractively() {
            // When
            Arguments args = readArguments(consoleInputFrom("i\nprompted-query\n"), "prompt-instance");

            // Then
            assertThat(args.query()).isEqualTo(QueryTrackerQuery.FOR_QUERY);
            assertThat(args.queryId()).isEqualTo("prompted-query");
        }

        @Test
        void shouldRepromptForQueryIdWhenBlankQueryIdEntered() {
            // When the query ID prompt is given an empty line and a whitespace line before a query ID
            Arguments args = readArguments(consoleInputFrom("i\n\n   \nprompted-query\n"), "prompt-instance");

            // Then
            assertThat(args.queryId()).isEqualTo("prompted-query");
        }

        @Test
        void shouldTrimWhitespaceAroundPromptedQueryId() {
            // When
            Arguments args = readArguments(consoleInputFrom("i\n  prompted-query  \n"), "prompt-instance");

            // Then
            assertThat(args.queryId()).isEqualTo("prompted-query");
        }

        @Test
        void shouldPromptForQueryIdWhenOptionSetWithBlankValue() {
            // When
            Arguments args = readArguments(consoleInputFrom("prompted-query\n"), "prompt-instance", "--query", " ");

            // Then
            assertThat(args.query()).isEqualTo(QueryTrackerQuery.FOR_QUERY);
            assertThat(args.queryId()).isEqualTo("prompted-query");
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
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "--all", "--failed"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, FAILED");
        }

        @Test
        void shouldRejectMultipleFlagsSetAsCombinedShortFlags() {
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "-qp"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Cannot combine query types. Options have been set for the following types: QUEUED, IN_PROGRESS");
        }

        @Test
        void shouldRejectQueryIdCombinedWithAnotherQueryType() {
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "--all", "--query", "my-query"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, FOR_QUERY");
        }

        @Test
        void shouldRejectQueryIdOptionWithNoValue() {
            assertThatThrownBy(() -> readArguments("for-query-instance", "--query"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasNoCause()
                    .hasMessage("Expected an argument for option: query");
        }

        @Test
        void shouldRejectMissingInstanceId() {
            assertThatThrownBy(() -> readArguments("--all"))
                    .isInstanceOf(CommandArgumentsException.class);
        }
    }

    private static QueryTrackerQuery queryFromArguments(String... args) {
        return readArguments(args).query();
    }

    private static QueryTrackerQuery queryFromArgumentsWithInput(String input, String... args) {
        return readArguments(consoleInputFrom(input), args).query();
    }

    private static Arguments readArguments(String... args) {
        return readArguments(consoleInputFrom(""), args);
    }

    private static Arguments readArguments(ConsoleInput input, String... args) {
        return QueryTrackerReport.readArguments(
                CommandArgumentReader.parse(QueryTrackerReport.USAGE, args), input);
    }

    private static ConsoleInput consoleInputFrom(String input) {
        return new ConsoleInput(null, new PrintStream(System.out),
                new Scanner(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))));
    }
}
