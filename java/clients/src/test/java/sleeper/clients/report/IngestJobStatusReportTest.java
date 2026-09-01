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
import sleeper.clients.report.job.query.JobQuery;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IngestJobStatusReportTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            Arguments args = readArguments("my-instance", "my-table");

            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.tableName()).isEqualTo("my-table");
            assertThat(args.outputType()).isEqualTo("STANDARD");
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
        void shouldReadQueryTypeUnfinishedFlag() {
            Arguments shortArgs = readArguments("unfinished-instance", "unfinished-table", "-u");
            assertThat(shortArgs.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);

            Arguments longArgs = readArguments("unfinished-instance", "unfinished-table", "--unfinished");
            assertThat(longArgs.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);
        }

        @Test
        void shouldReadOutputTypeJson() {
            Arguments args = readArguments("json-instance", "json-table", "--output-type", "json");
            assertThat(args.outputType()).isEqualTo("JSON");
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

    private static Arguments readArguments(String... args) {
        return IngestJobStatusReport.readArguments(CommandArgumentReader.parse(IngestJobStatusReport.USAGE, args));
    }

}
