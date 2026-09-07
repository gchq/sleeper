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
import sleeper.clients.report.ingest.job.IngestJobStatusReporter;
import sleeper.clients.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.job.query.AllJobsQuery;
import sleeper.clients.report.job.query.DetailedJobsQuery;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.report.job.query.UnfinishedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.table.TableStatus;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IngestJobStatusReportTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            Arguments args = readArguments("my-instance", "my-table", "--all");

            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.tableName()).isEqualTo("my-table");
            assertThat(args.reporter()).isInstanceOf(StandardIngestJobStatusReporter.class);
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.ALL);
        }

        @Test
        void shouldReadQueryTypeAllFlag() {
            Arguments shortArgs = readArguments("all-instance", "all-table", "-a");
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.ALL);

            Arguments longArgs = readArguments("all-instance", "all-table", "--all");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.ALL);
        }

        @Test
        void shouldReadQueryTypeDetailedFlag() {
            Arguments shortArgs = readArguments("detailed-instance", "detailed-table", "-d", "23");
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.DETAILED);

            Arguments longArgs = readArguments("detailed-instance", "detailed-table", "--detailed", "5871");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.DETAILED);
        }

        @Test
        void shouldReadQueryTypeRejectedFlag() {
            Arguments shortArgs = readArguments("rejected-instance", "rejected-table", "-n");
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.REJECTED);

            Arguments longArgs = readArguments("rejected-instance", "rejected-table", "--rejected");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.REJECTED);
        }

        @Test
        void shoudlReadQueryTypeRangeFlag() {
            Arguments shortArgs = readArguments("range-instance", "range-table", "-r");
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.RANGE);

            Arguments longArgs = readArguments("range-instance", "range-table", "--range");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.RANGE);
        }

        @Test
        void shouldReadQueryTypeRangeWhenOnlyStartTimeEndTimeFlagsGiven() {
            Arguments args = readArguments("start-end-instance", "start-end-table",
                    "--start-time", "20201114120101",
                    "--end-time", "20210407150000");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.RANGE);
        }

        @Test
        void shouldReadQueryTypeUnfinishedFlag() {
            Arguments shortArgs = readArguments("unfinished-instance", "unfinished-table", "-u");
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);

            Arguments longArgs = readArguments("unfinished-instance", "unfinished-table", "--unfinished");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);
        }

        @Test
        void shouldReadOutputTypeJson() {
            Arguments args = readArguments("json-instance", "json-table", "--output-type", "json");
            assertThat(args.reporter()).isInstanceOf(JsonIngestJobStatusReporter.class);
        }

        @Test
        void shouldReturnPromptQueryTypeWhenNoFlagSet() {
            Arguments args = readArguments("prompt-instance", "prompt-table");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.PROMPT);
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            assertThatThrownBy(() -> readArguments("my-instance", "my-table", "--output-type", "BAD-REPORT"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Output type not supported: BAD-REPORT. Valid types: JSON, STANDARD");
        }

        @Test
        void shouldRejectMultipleFlagsSet() {
            assertThatThrownBy(() -> readArguments("multiple-flag-instance", "multiple-flag-table", "--all", "--unfinished"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Too many query type flags are set, maximum of 1. Flags set: ALL, UNFINISHED");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectAllQueryWithTimeFlagsSet() {
            assertThatThrownBy(() -> readArguments("all-time-instance", "all-time-table", "--all",
                    "--start-time", "20220417053218",
                    "--end-time", "20241122120001"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: ALL");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectDetailedQueryWithTimeFlagsSet() {
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--detailed", "84916",
                    "--start-time", "20251112140000",
                    "--end-time", "20260101152929"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: DETAILED");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectRejectedQueryWithTimeFlagsSet() {
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--rejected",
                    "--start-time", "20231225120000",
                    "--end-time", "20231228120000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: REJECTED");
        }

        // Will need be removed as part of work for https://github.com/gchq/sleeper/issues/8061
        @Test
        void shouldRejectUnfinishedQueryWithTimeFlagsSet() {
            assertThatThrownBy(() -> readArguments("detailed-time-instance", "detailed-time-table", "--unfinished",
                    "--start-time", "20260901180000",
                    "--end-time", "20260902175959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range time flags, start-time and end-time are not valid for following query type: UNFINISHED");
        }

        @Test
        void shouldRejectDetailedReportWithoutInstanceId() {
            assertThatThrownBy(() -> readArguments("detail-fail-instance", "detail-fail-table", "-d"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected an argument for option: detailed");
        }

        @Test
        void shouldRejectRangeReportWithInvalidateDateFormatStartTime() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "asdad", "--end-time", "20150411084545"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("start-time parameter don't match expected format: yyyyMMddHHmmss");
        }

        @Test
        void shouldRejectRangeReportWithInvalidateDateFormatEndTime() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20170404152121", "--end-time", "gdsd"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("end-time parameter don't match expected format: yyyyMMddHHmmss");
        }

        @Test
        void shouldRejectRangeReportWithEndTimeBeforeStartTime() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20200101120000", "--end-time", "19700101120000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range end is before range start. Range start: 20200101120000, range end: 19700101120000");
        }

        @Test
        void shouldRejectRangeReportWithStartTimeButNoEndTime() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--start-time", "20221101085959"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing paramter of end-time which is required for the ranged query type.");
        }

        @Test
        void shouldRejectRangeReportWithEndTimeButStartTime() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r",
                    "--end-time", "20240912093000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Missing paramter of start-time which is required for the ranged query type.");
        }
    }

    @Nested
    class QueryParametersGeneration {

        @Test
        void shouldGenerateCorrectParamsForDetailedType() {
            // Given
            Arguments args = readArguments("detailed-params-instance", "detailed-params-table", "--detailed", "151958191");

            // When / Then
            assertThat(IngestJobStatusReport.determineQueryParams(args)).isEqualTo("151958191");
        }

        @Test
        void shouldGenerateCorrectParamsForRangeType() {
            // Given
            Arguments args = readArguments("range-params-instance", "range-params-table", "--range", "--start-time", "20200809152311", "--end-time", "20210403111111");

            // When / Then
            assertThat(IngestJobStatusReport.determineQueryParams(args)).isEqualTo("20200809152311,20210403111111");
        }

        @Test
        void shouldCreateNullParamsForTypesThatDontRequireQueryParams() {
            assertThat(IngestJobStatusReport.determineQueryParams(
                    readArguments("all-params-instance", "all-params-table", "--all"))).isNull();

            assertThat(IngestJobStatusReport.determineQueryParams(
                    readArguments("unfinished-params-instance", "unfinished-params-table", "--unfinished"))).isNull();

            assertThat(IngestJobStatusReport.determineQueryParams(
                    readArguments("rejected-params-instance", "rejected-params-table", "--rejected"))).isNull();
        }
    }

    @Nested
    class JobQueryCreation {

        IngestJobStatusReporter reporter = new StandardIngestJobStatusReporter();

        @Test
        void shouldCreateValidAllJobsQuery() {
            // Given
            String tableName = "all-job-table";
            AllJobsQuery allJobsQuery = new AllJobsQuery(createTableStatus(tableName));

            // When
            JobQuery jobFromArgs = createJobQueryFromArguments(
                    new Arguments("all-job-instance",
                            tableName,
                            reporter,
                            JobQuery.Type.ALL,
                            null, null, null));

            // Then
            assertThat(jobFromArgs).isEqualTo(allJobsQuery);
        }

        @Test
        void shouldCreateValidDetailedJobsQuery() {
            // Given
            String jobId = "6545";
            DetailedJobsQuery detailedJobsQuery = new DetailedJobsQuery(List.of(jobId));

            // When
            JobQuery jobFromArgs = createJobQueryFromArguments(
                    new Arguments("detailed-job-instance",
                            "detailed-job-table",
                            reporter,
                            JobQuery.Type.DETAILED,
                            jobId,
                            null, null));

            // Then
            assertThat(jobFromArgs).isEqualTo(detailedJobsQuery);
        }

        @Test
        void shouldCreateValidRangeJobsQuery() {
            // Given
            String tableName = "range-job-table";
            Instant startTime = Instant.parse("2020-10-10T09:30:00Z");
            Instant endTime = Instant.parse("2021-10-08T15:00:00Z");
            RangeJobsQuery rangeJobsQuery = new RangeJobsQuery(createTableStatus(tableName), startTime, endTime);

            // When
            JobQuery jobFromArgs = createJobQueryFromArguments(
                    new Arguments("range-job-instance",
                            tableName,
                            reporter,
                            JobQuery.Type.RANGE,
                            null,
                            "20201010093000", "20211008150000"));

            // Then
            assertThat(jobFromArgs).isEqualTo(rangeJobsQuery);
        }

        @Test
        void shouldCreateValidUnfinishedJobsQuery() {
            // Given
            String tableName = "unfinished-job-table";
            UnfinishedJobsQuery unfinishedJobsQuery = new UnfinishedJobsQuery(createTableStatus(tableName));

            // When
            JobQuery jobFromArgs = createJobQueryFromArguments(
                    new Arguments("unfinished-job-instance",
                            tableName,
                            reporter,
                            JobQuery.Type.UNFINISHED,
                            null, null, null));

            // Then
            assertThat(jobFromArgs).isEqualTo(unfinishedJobsQuery);
        }

        @Test
        void shouldCreateValidRejectedJobsQuery() {
            // Given
            RejectedJobsQuery rejectedJobsQuery = new RejectedJobsQuery();

            // When
            JobQuery jobFromArgs = createJobQueryFromArguments(
                    new Arguments("rejected-job-instance",
                            "rejected-job-table",
                            reporter,
                            JobQuery.Type.REJECTED,
                            null, null, null));

            // Then
            assertThat(jobFromArgs).isEqualTo(rejectedJobsQuery);
        }

        private JobQuery createJobQueryFromArguments(Arguments args) {
            return IngestJobStatusReport.queryfromParametersOrPrompt(createTableStatus(args.tableName()),
                    args.queryType(),
                    IngestJobStatusReport.determineQueryParams(args),
                    Clock.systemUTC(),
                    ConsoleInput.stdIn());
        }

        private TableStatus createTableStatus(String tableName) {
            return TableStatus.uniqueIdAndName(tableName, tableName, Boolean.TRUE);
        }
    }

    private static Arguments readArguments(String... args) {
        return IngestJobStatusReport.readArguments(CommandArgumentReader.parse(IngestJobStatusReport.USAGE, args));
    }

}
