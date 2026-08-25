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
            assertThat(args.reportType()).isEqualTo("STANDARD");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.ALL);
            assertThat(args.queryParameters()).isNull();
        }

        @Test
        void shouldReadQueryTypeAllFlag() {
            Arguments args = readArguments("all-instance", "all-table", "-a");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.ALL);
        }

        @Test
        void shouldReadQueryTypeDetailedFlag() {
            Arguments args = readArguments("detailed-instance", "detailed-table", "-d", "--query-paramerters", "23");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.DETAILED);
        }

        @Test
        void shouldReadQueryTypeRejectedFlag() {
            Arguments args = readArguments("rejected-instance", "rejected-table", "-n");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.REJECTED);
        }

        @Test
        void shoudlReadQueryTypeRangeFlag() {
            Arguments args = readArguments("range-instance", "range-table", "-r", "--query-parameters", "20200101120000,20220101120000");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.RANGE);
        }

        @Test
        void shouldReadQueryTypeUnfinishedFlag() {
            Arguments args = readArguments("unfinished-instance", "unfinished-table", "-u");
            assertThat(args.queryType()).isEqualTo(JobQuery.Type.UNFINISHED);
        }

        @Test
        void shouldReadReportTypeJson() {
            Arguments args = readArguments("json-instance", "json-table", "--report-type", "json");
            assertThat(args.reportType()).isEqualTo("JSON");
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            assertThatThrownBy(() -> readArguments("my-instance", "my-table", "--report-type", "BAD-REPORT"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Report type not supported: BAD-REPORT. Valid types: JSON, STANDARD");
        }

        @Test
        void shouldRejectDetailedReportWithoutInstanceId() {
            assertThatThrownBy(() -> readArguments("detail-fail-instance", "detail-fail-table", "-d"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Query parameters are required for the query type: DETAILED");
        }

        @Test
        void shouldRejectRangeReportWithoutQueryParameters() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Query parameters are required for the query type: RANGE");
        }

        @Test
        void shouldRejectRangeReportWithInvalidateDateFormat() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r", "--query-parameters", "asdad,wqas"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range parameters don't match expected format: yyyyMMddHHmmss");
        }

        @Test
        void shouldRejectRangeReportWithEndDateBeforeStartDate() {
            assertThatThrownBy(() -> readArguments("range-fail-instance", "range-fail-table", "-r", "--query-parameters", "20200101120000,19700101120000"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Range end is before rage start. Range start: 20200101120000, range end: 19700101120000");
        }
    }

    private static Arguments readArguments(String... args) {
        return IngestJobStatusReport.readArguments(CommandArgumentReader.parse(IngestJobStatusReport.USAGE, args));
    }

}
