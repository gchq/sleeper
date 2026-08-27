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

import sleeper.clients.report.ListTablesReport.Arguments;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ListTableReportTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            Arguments args = readArguments("test-instance");

            assertThat(args.instanceId()).isEqualTo("test-instance");
            assertThat(args.reportType()).isEqualTo("STANDARD");
        }

        @Test
        void shouldReadReportType() {
            Arguments args = readArguments("json-instance", "--report-type", "json");

            assertThat(args.reportType()).isEqualTo("JSON");
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            assertThatThrownBy(() -> readArguments("fail-instance", "--report-type", "broken-report-type"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Report type not supported: BROKEN-REPORT-TYPE. Valid types: JSON, STANDARD");
        }
    }

    private static Arguments readArguments(String... args) {
        return ListTablesReport.readArguments(CommandArgumentReader.parse(ListTablesReport.USAGE, args));
    }

}
