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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.report.IngestJobStatusReport.Arguments;
import sleeper.clients.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.ALL);
            assertThat(args.jobId()).isNull();
            assertThat(args.startTime()).isNull();
            assertThat(args.endTime()).isNull();
        }

        @Test
        void shouldReadQueryTypeAllFlag() {
            // When
            Arguments shortArgs = readArguments("all-instance", "all-table", "-a");
            Arguments longArgs = readArguments("all-instance", "all-table", "--all");

            // Then
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.ALL);
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.ALL);
        }

        @Test
        void shouldReadQueryTypeDetailedFlag() {
            // When
            Arguments shortArgs = readArguments("detailed-instance", "detailed-table", "-d", "23");
            Arguments longArgs = readArguments("detailed-instance", "detailed-table", "--detailed", "5871");

            // Then
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.DETAILED);
            assertThat(shortArgs.jobId()).isEqualTo("23");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.DETAILED);
            assertThat(longArgs.jobId()).isEqualTo("5871");
        }

        @Test
        void shouldReadJobIdAttachedToShortOption() {
            // When
            Arguments args = readArguments("detailed-instance", "detailed-table", "-d23");

            // Then
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.DETAILED);
            assertThat(args.jobId()).isEqualTo("23");
        }

        @Test
        void shouldReadJobIdThatLooksLikeAnOption() {
            // When
            Arguments args = readArguments("detailed-instance", "detailed-table", "-d", "-a");

            // Then
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.DETAILED);
            assertThat(args.jobId()).isEqualTo("-a");
        }

        @Test
        void shouldReadQueryTypeRejectedFlag() {
            // When
            Arguments shortArgs = readArguments("rejected-instance", "rejected-table", "-n");
            Arguments longArgs = readArguments("rejected-instance", "rejected-table", "--rejected");

            // Then
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.REJECTED);
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.REJECTED);
        }

        @Test
        void shouldReadQueryTypeRangeFlag() {
            // When
            Arguments shortArgs = readArguments("range-instance", "range-table", "-r");
            Arguments longArgs = readArguments("range-instance", "range-table", "--range");

            // Then
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.RANGE);
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.RANGE);
        }

        @Test
        void shouldReadQueryTypeRangeWhenOnlyStartTimeEndTimeFlagsGiven() {
            // When
            Arguments args = readArguments("start-end-instance", "start-end-table",
                    "--start-time", "20201114120101",
                    "--end-time", "20210407150000");

            // Then
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.RANGE);
            assertThat(args.startTime()).isEqualTo(Instant.parse("2020-11-14T12:01:01Z"));
            assertThat(args.endTime()).isEqualTo(Instant.parse("2021-04-07T15:00:00Z"));
        }

        @Test
        void shouldReadQueryTypeRangeWhenRangeFlagSetToTrue() {
            // When
            Arguments args = readArguments("range-instance", "range-table", "--range=true");

            // Then
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.RANGE);
        }

        @Test
        void shouldReadQueryTypePromptWhenRangeFlagSetToFalse() {
            // When
            Arguments args = readArguments("range-instance", "range-table", "--range=false");

            // Then
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.PROMPT);
        }

        @Test
        void shouldReadQueryTypeUnfinishedFlag() {
            // When
            Arguments shortArgs = readArguments("unfinished-instance", "unfinished-table", "-u");
            Arguments longArgs = readArguments("unfinished-instance", "unfinished-table", "--unfinished");

            // Then
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);
        }

        @Test
        void shouldReadReportTypeJson() {
            // When
            Arguments args = readArguments("json-instance", "json-table", "--report-type", "json");

            // Then
            assertThat(args.reporter()).isInstanceOf(JsonIngestJobStatusReporter.class);
        }

        @Test
        void shouldReturnPromptQueryTypeWhenNoFlagSet() {
            // When
            Arguments args = readArguments("prompt-instance", "prompt-table");

            // Then
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.PROMPT);
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
                    .hasMessage("Too many query type flags are set, maximum of 1. Flags set: ALL, UNFINISHED");
        }

        @Test
        void shouldRejectMultipleFlagsSetAsCombinedShortFlags() {
            // When / Then
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "multiple-flag-table", "-au"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Too many query type flags are set, maximum of 1. Flags set: ALL, UNFINISHED");
        }

        @Test
        void shouldListEveryQueryTypeSetInTheOrderTheyAppearInTheUsage() {
            // When / Then
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "multiple-flag-table", "-aur"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Too many query type flags are set, maximum of 1. Flags set: ALL, RANGE, UNFINISHED");
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
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: ALL");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectDetailedQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--detailed", "84916",
                    "--start-time", "20251112140000",
                    "--end-time", "20260101152929"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: DETAILED");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectRejectedQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--rejected",
                    "--start-time", "20231225120000",
                    "--end-time", "20231228120000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: REJECTED");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectUnfinishedQueryWithTimeFlagsSet() {
            // When / Then
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--unfinished",
                    "--start-time", "20260901180000",
                    "--end-time", "20260902175959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: UNFINISHED");
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
                    .hasMessage("Range end is before range start. Range start: 20200101120000, range end: 19700101120000");
        }

        @Test
        void shouldRejectRangeReportWithStartTimeButNoEndTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20221101085959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of end-time which is required for the Range query type.");
        }

        @Test
        void shouldRejectRangeReportWithEndTimeButNoStartTime() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--end-time", "20240912093000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of start-time which is required for the Range query type.");
        }

        @Test
        void shouldReportMissingEndTimeWhenOnlyStartTimeGivenWithNoQueryTypeFlag() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table",
                    "--start-time", "20221101085959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of end-time which is required for the Range query type.");
        }

        @Test
        void shouldReportMissingStartTimeWhenOnlyEndTimeGivenWithNoQueryTypeFlag() {
            // When / Then
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table",
                    "--end-time", "20240912093000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing parameter of start-time which is required for the Range query type.");
        }
    }

    /**
     * Checks that the arguments produce a query that asks the job tracker the right question. Drives the whole path
     * from the command line, so that the query type, the query parameters and the query are all covered together.
     */
    @Nested
    class JobQueryCreation {

        private static final String TABLE_ID = "test-table-id";

        private final IngestJobTracker tracker = mock(IngestJobTracker.class);

        @Test
        void shouldQueryAllJobs() {
            // When
            runQueryFromArguments("all-job-instance", "all-job-table", "--all");

            // Then
            verify(tracker).getAllJobs(TABLE_ID);
            verifyNoMoreInteractions(tracker);
        }

        @Test
        void shouldQueryUnfinishedJobs() {
            // When
            runQueryFromArguments("unfinished-job-instance", "unfinished-job-table", "--unfinished");

            // Then
            verify(tracker).getUnfinishedJobs(TABLE_ID);
            verifyNoMoreInteractions(tracker);
        }

        @Test
        void shouldQueryRejectedJobs() {
            // When
            runQueryFromArguments("rejected-job-instance", "rejected-job-table", "--rejected");

            // Then
            verify(tracker).getInvalidJobs();
            verifyNoMoreInteractions(tracker);
        }

        @Test
        void shouldQueryJobWithGivenId() {
            // When
            runQueryFromArguments("detailed-job-instance", "detailed-job-table", "--detailed", "6545");

            // Then
            verify(tracker).getJob("6545");
            verifyNoMoreInteractions(tracker);
        }

        @Test
        void shouldQueryEachJobWhenSeveralIdsGivenSeparatedByCommas() {
            // When
            runQueryFromArguments("detailed-job-instance", "detailed-job-table", "--detailed", "6545,8102");

            // Then
            verify(tracker).getJob("6545");
            verify(tracker).getJob("8102");
            verifyNoMoreInteractions(tracker);
        }

        @Test
        void shouldQueryJobsInGivenPeriod() {
            // When
            runQueryFromArguments("range-job-instance", "range-job-table",
                    "--range", "--start-time", "20201010093000", "--end-time", "20211008150000");

            // Then
            verify(tracker).getJobsInTimePeriod(TABLE_ID,
                    Instant.parse("2020-10-10T09:30:00Z"), Instant.parse("2021-10-08T15:00:00Z"));
            verifyNoMoreInteractions(tracker);
        }

        @Test
        void shouldQueryJobsInLastFourHoursWhenRangeSetWithNoTimes() {
            // Given
            Instant now = Instant.parse("2024-05-01T12:00:00Z");

            // When
            runQueryFromArgumentsAtTime(now, "range-default-instance", "range-default-table", "-r");
            runQueryFromArgumentsAtTime(now, "range-default-instance", "range-default-table", "--range");

            // Then
            verify(tracker, times(2)).getJobsInTimePeriod(TABLE_ID,
                    Instant.parse("2024-05-01T08:00:00Z"), now);
            verifyNoMoreInteractions(tracker);
        }

        private void runQueryFromArguments(String... args) {
            runQueryFromArgumentsAtTime(Instant.now(), args);
        }

        private void runQueryFromArgumentsAtTime(Instant now, String... args) {
            Arguments arguments = readArguments(args);
            IngestJobStatusReport.createQuery(arguments, Clock.fixed(now, ZoneId.of("UTC")), ConsoleInput.stdIn())
                    .run(tracker, TABLE_ID);
        }
    }

    private static Arguments readArguments(String... args) {
        return IngestJobStatusReport.readArguments(CommandArgumentReader.parse(IngestJobStatusReport.USAGE, args));
    }

}
