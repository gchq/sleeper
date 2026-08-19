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

import sleeper.clients.report.FilesStatusReport.Arguments;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FilesStatusReportMainTest {

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            Arguments args = readArguments("my-instance", "my-table");

            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.tableName()).isEqualTo("my-table");
            assertThat(args.maxNoRefFiles()).isEqualTo(1000);
            assertThat(args.verbose()).isFalse();
            assertThat(args.reporterType()).isEqualTo("STANDARD");
        }

        @Test
        void shouldReadMaxNoRefFiles() {
            Arguments args = readArguments("my-instance", "my-table", "--max-no-ref-files", "500");

            assertThat(args.maxNoRefFiles()).isEqualTo(500);
        }

        @Test
        void shouldReadVerboseFlag() {
            Arguments args = readArguments("my-instance", "my-table", "--verbose");

            assertThat(args.verbose()).isTrue();
        }

        @Test
        void shouldReadReportTypeJson() {
            Arguments args = readArguments("my-instance", "my-table", "--report-type", "json");

            assertThat(args.reporterType()).isEqualTo("JSON");
        }

        @Test
        void shouldReadReportTypeCsv() {
            Arguments args = readArguments("my-instance", "my-table", "--report-type", "csv");

            assertThat(args.reporterType()).isEqualTo("CSV");
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            assertThatThrownBy(() -> readArguments("my-instance", "my-table", "--report-type", "unknown"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessageContaining("Report type not supported: UNKNOWN");
        }
    }

    private static Arguments readArguments(String... args) {
        return FilesStatusReport.readArguments(CommandArgumentReader.parse(FilesStatusReport.USAGE, args));
    }
}
