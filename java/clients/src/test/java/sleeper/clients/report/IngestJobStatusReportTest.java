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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.report.IngestJobStatusReport.Arguments;
import sleeper.clients.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.job.query.AllJobsQuery;
import sleeper.clients.report.job.query.DetailedJobsQuery;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.report.job.query.UnfinishedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IngestJobStatusReportTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            // When
            Arguments args = readArguments("my-instance", "my-table", "--all");

            // Then
            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.tableName()).isEqualTo("my-table");
            assertThat(args.reporter()).isInstanceOf(StandardIngestJobStatusReporter.class);
        }

        @Test
        void shouldReadReportTypeJson() {
            // When
            Arguments args = readArguments("json-instance", "json-table", "--report-type", "json", "--all");

            // Then
            assertThat(args.reporter()).isInstanceOf(JsonIngestJobStatusReporter.class);
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            // When / Then
            assertThatThrownBy(() -> readArguments("my-instance", "my-table", "--report-type", "BAD-REPORT"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Report type not supported: BAD-REPORT. Valid types: STANDARD, JSON");
        }

        @Test
        void shouldRejectMultipleFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "multiple-flag-table", "--all", "--unfinished"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, UNFINISHED");
        }

        @Test
        void shouldRejectMultipleFlagsSetAsCombinedShortFlags() {
            // When / Then
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "multiple-flag-table", "-au"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, UNFINISHED");
        }

        @Test
        void shouldListEveryQueryTypeSetInTheOrderTheyAppearInTheUsage() {
            // When / Then
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "multiple-flag-table", "-aur"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, RANGE, UNFINISHED");
        }

        @Test
        void shouldRejectDetailedReportWithEmptyJobId() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detail-fail-instance", "detail-fail-table", "--detailed="))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected a value for option: detailed");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectAllQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("all-time-instance", "all-time-table", "--all",
                    "--start-time", "20220417053218",
                    "--end-time", "20241122120001"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: ALL, RANGE");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectDetailedQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--detailed", "84916",
                    "--start-time", "20251112140000",
                    "--end-time", "20260101152929"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: DETAILED, RANGE");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectRejectedQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--rejected",
                    "--start-time", "20231225120000",
                    "--end-time", "20231228120000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: RANGE, REJECTED");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectUnfinishedQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--unfinished",
                    "--start-time", "20260901180000",
                    "--end-time", "20260902175959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Cannot combine query types. Options have been set for the following types: RANGE, UNFINISHED");
        }

        @Test
        void shouldRejectDetailedReportWithoutJobId() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detail-fail-instance", "detail-fail-table", "-d"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected an argument for option: detailed");
        }

        @Test
        void shouldRejectRangeReportWithInvalidDateFormatStartTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "asdad", "--end-time", "20150411084545"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("start-time parameter doesn't match expected format: yyyyMMddHHmmss");
        }

        @Test
        void shouldRejectRangeReportWithInvalidDateFormatEndTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20170404152121", "--end-time", "gdsd"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("end-time parameter doesn't match expected format: yyyyMMddHHmmss");
        }

        @Test
        void shouldRejectRangeReportWithEndTimeBeforeStartTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20200101120000", "--end-time", "19700101120000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range end is before range start. Range start: 2020-01-01T12:00:00Z, range end: 1970-01-01T12:00:00Z");
        }

        @Test
        @Disabled("TODO")
        void shouldRejectRangeReportWithStartTimeButNoEndTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20221101085959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of end-time which is required for the Range query type.");
        }

        @Test
        @Disabled("TODO")
        void shouldRejectRangeReportWithEndTimeButNoStartTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--end-time", "20240912093000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of start-time which is required for the Range query type.");
        }

        @Test
        @Disabled("TODO")
        void shouldReportMissingEndTimeWhenOnlyStartTimeGivenWithNoQueryTypeFlag() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table",
                    "--start-time", "20221101085959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of end-time which is required for the Range query type.");
        }

        @Test
        @Disabled("TODO")
        void shouldReportMissingStartTimeWhenOnlyEndTimeGivenWithNoQueryTypeFlag() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table",
                    "--end-time", "20240912093000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of start-time which is required for the Range query type.");
        }
    }

    /**
     * Checks that the arguments produce a query that asks the job tracker the right question. Asserts directly on the
     * query object, so that the query type and parameters are all covered together.
     */
    @Nested
    class JobQueryCreation {

        @Test
        void shouldQueryAllJobs() {
            assertThat(queryFromArguments("all-job-instance", "all-job-table", "--all"))
                    .usingRecursiveComparison()
                    .isEqualTo(new AllJobsQuery());
        }

        @Test
        void shouldQueryAllJobsWithShortFlag() {
            assertThat(queryFromArguments("all-job-instance", "all-job-table", "-a"))
                    .usingRecursiveComparison()
                    .isEqualTo(new AllJobsQuery());
        }

        @Test
        void shouldQueryUnfinishedJobs() {
            assertThat(queryFromArguments("unfinished-job-instance", "unfinished-job-table", "--unfinished"))
                    .usingRecursiveComparison()
                    .isEqualTo(new UnfinishedJobsQuery());
        }

        @Test
        void shouldQueryUnfinishedJobsWithShortFlag() {
            assertThat(queryFromArguments("unfinished-job-instance", "unfinished-job-table", "-u"))
                    .usingRecursiveComparison()
                    .isEqualTo(new UnfinishedJobsQuery());
        }

        @Test
        void shouldQueryRejectedJobs() {
            assertThat(queryFromArguments("rejected-job-instance", "rejected-job-table", "--rejected"))
                    .usingRecursiveComparison()
                    .isEqualTo(new RejectedJobsQuery());
        }

        @Test
        void shouldQueryRejectedJobsWithShortFlag() {
            assertThat(queryFromArguments("rejected-job-instance", "rejected-job-table", "-n"))
                    .usingRecursiveComparison()
                    .isEqualTo(new RejectedJobsQuery());
        }

        @Test
        void shouldQueryJobWithGivenId() {
            assertThat(queryFromArguments("detailed-job-instance", "detailed-job-table", "--detailed", "6545"))
                    .usingRecursiveComparison()
                    .isEqualTo(new DetailedJobsQuery(List.of("6545")));
        }

        @Test
        void shouldQueryDetailedJobWithShortFlag() {
            assertThat(queryFromArguments("detailed-job-instance", "detailed-job-table", "-d", "23"))
                    .usingRecursiveComparison()
                    .isEqualTo(new DetailedJobsQuery(List.of("23")));
        }

        @Test
        void shouldQueryDetailedJobWithIdAttachedToShortOption() {
            assertThat(queryFromArguments("detailed-job-instance", "detailed-job-table", "-d23"))
                    .usingRecursiveComparison()
                    .isEqualTo(new DetailedJobsQuery(List.of("23")));
        }

        @Test
        void shouldQueryJobWithIdThatLooksLikeAnOption() {
            assertThat(queryFromArguments("detailed-job-instance", "detailed-job-table", "-d", "-a"))
                    .usingRecursiveComparison()
                    .isEqualTo(new DetailedJobsQuery(List.of("-a")));
        }

        @Test
        void shouldQueryEachJobWhenSeveralIdsGivenSeparatedByCommas() {
            assertThat(queryFromArguments("detailed-job-instance", "detailed-job-table", "--detailed", "6545,8102"))
                    .usingRecursiveComparison()
                    .isEqualTo(new DetailedJobsQuery(List.of("6545", "8102")));
        }

        @Test
        void shouldQueryJobsInGivenPeriod() {
            assertThat(queryFromArguments("range-job-instance", "range-job-table",
                    "--range", "--start-time", "20201010093000", "--end-time", "20211008150000"))
                    .usingRecursiveComparison()
                    .isEqualTo(new RangeJobsQuery(
                            Instant.parse("2020-10-10T09:30:00Z"), Instant.parse("2021-10-08T15:00:00Z")));
        }

        @Test
        void shouldQueryJobsInGivenPeriodWhenOnlyTimeFlagsGiven() {
            assertThat(queryFromArguments("range-job-instance", "range-job-table",
                    "--start-time", "20201114120101", "--end-time", "20210407150000"))
                    .usingRecursiveComparison()
                    .isEqualTo(new RangeJobsQuery(
                            Instant.parse("2020-11-14T12:01:01Z"), Instant.parse("2021-04-07T15:00:00Z")));
        }

        @Test
        void shouldQueryJobsInLastFourHoursWhenRangeSetWithNoTimes() {
            // Given
            Instant now = Instant.parse("2024-05-01T12:00:00Z");
            RangeJobsQuery expectedQuery = new RangeJobsQuery(
                    Instant.parse("2024-05-01T08:00:00Z"), now);

            // Then
            assertThat(queryFromArgumentsAtTime(now, "range-default-instance", "range-default-table", "-r"))
                    .usingRecursiveComparison()
                    .isEqualTo(expectedQuery);
            assertThat(queryFromArgumentsAtTime(now, "range-default-instance", "range-default-table", "--range"))
                    .usingRecursiveComparison()
                    .isEqualTo(expectedQuery);
        }

        @Test
        void shouldQueryJobsInDefaultPeriodWhenRangeSetToTrue() {
            // Given
            Instant now = Instant.parse("2024-05-01T12:00:00Z");

            // Then
            assertThat(queryFromArgumentsAtTime(now, "range-instance", "range-table", "--range=true"))
                    .usingRecursiveComparison()
                    .isEqualTo(new RangeJobsQuery(
                            Instant.parse("2024-05-01T08:00:00Z"), now));
        }

        @Test
        void shouldPromptForQueryTypeWhenNoFlagSet() {
            assertThat(queryFromArgumentsWithInput("a\n", "prompt-instance", "prompt-table"))
                    .usingRecursiveComparison()
                    .isEqualTo(new AllJobsQuery());
        }

        @Test
        void shouldPromptForQueryTypeWhenRangeFlagSetToFalse() {
            assertThat(queryFromArgumentsWithInput("a\n", "range-instance", "range-table", "--range=false"))
                    .usingRecursiveComparison()
                    .isEqualTo(new AllJobsQuery());
        }

        private JobQuery queryFromArguments(String... args) {
            return queryFromArgumentsAtTime(Instant.now(), args);
        }

        private JobQuery queryFromArgumentsAtTime(Instant now, String... args) {
            return readArgumentsAtTime(now, ConsoleInput.stdIn(), args).query();
        }

        private JobQuery queryFromArgumentsWithInput(String input, String... args) {
            return readArgumentsAtTime(Instant.now(), consoleInputFrom(input), args).query();
        }
    }

    private static Arguments readArguments(String... args) {
        return readArgumentsAtTime(Instant.now(), ConsoleInput.stdIn(), args);
    }

    private static Arguments readArgumentsAtTime(Instant now, ConsoleInput input, String... args) {
        return IngestJobStatusReport.readArguments(
                CommandArgumentReader.parse(IngestJobStatusReport.USAGE, args),
                () -> now, input);
    }

    private static ConsoleInput consoleInputFrom(String input) {
        return new ConsoleInput(null, new PrintStream(System.out),
                new Scanner(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))));
    }

}
