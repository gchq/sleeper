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

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.report.FilesStatusReport.Arguments;
import sleeper.clients.report.filestatus.CsvFileStatusReporter;
import sleeper.clients.report.filestatus.FileStatusDetailLevel;
import sleeper.clients.report.filestatus.JsonFileStatusReporter;
import sleeper.clients.report.filestatus.StandardFileStatusReporter;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.util.Locale;
import java.util.stream.Stream;

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
            assertThat(args.detailLevel()).isEqualTo(FileStatusDetailLevel.STATS_ONLY);
            assertThat(args.reporter())
                    .usingRecursiveComparison()
                    .isEqualTo(new StandardFileStatusReporter());
            assertThat(args.useJobTracker()).isTrue();
        }

        @Test
        void shouldDisableJobTracker() {
            Arguments args = readArguments("my-instance", "my-table", "--job-tracker=false");

            assertThat(args.useJobTracker()).isFalse();
        }

        @Test
        void shouldReadMaxNoRefFiles() {
            Arguments args = readArguments("my-instance", "my-table", "--max-no-ref-files", "500");

            assertThat(args.maxNoRefFiles()).isEqualTo(500);
        }

        @Test
        void shouldReadDetailLevel() {
            Arguments args = readArguments("my-instance", "my-table", "--detail", "compaction_jobs");

            assertThat(args.detailLevel()).isEqualTo(FileStatusDetailLevel.COMPACTION_JOBS);
        }

        @Test
        void shouldReadReportTypeJson() {
            Arguments args = readArguments("my-instance", "my-table", "--format", "json");

            assertThat(args.reporter())
                    .usingRecursiveComparison()
                    .ignoringFields("gson")
                    .isEqualTo(new JsonFileStatusReporter());
        }

        @Test
        void shouldReadReportTypeCsv() {
            Arguments args = readArguments("my-instance", "my-table", "--format", "csv");

            assertThat(args.reporter())
                    .usingRecursiveComparison()
                    .isEqualTo(new CsvFileStatusReporter());
        }
    }

    @Nested
    class HelpText {

        @Test
        void shouldOutputHelpText() {
            Approvals.verify(FilesStatusReport.USAGE.createHelpText(),
                    new Options().forFile().withBaseName("FilesStatusReportHelpText"));
        }

        @Test
        void shouldIncludeAllDetailOptionsInHelpText() {
            // When
            String helpText = FilesStatusReport.USAGE.createHelpText();

            // Then
            assertThat(helpText).contains(Stream.of(FileStatusDetailLevel.values())
                    .map(level -> level.toString().toLowerCase(Locale.ROOT))
                    .toList());
        }
    }

    @Nested
    class ArgumentsValidation {

        @Test
        void shouldRejectUnknownReportType() {
            assertThatThrownBy(() -> readArguments("my-instance", "my-table", "--format", "unknown"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Output format not supported: unknown. Valid formats: CSV, JSON, STANDARD");
        }

        @Test
        void shouldRejectUnknownDetailLevel() {
            assertThatThrownBy(() -> readArguments("my-instance", "my-table", "--detail", "unknown"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Detail level not supported: unknown. Valid levels: stats_only, compaction_jobs, references, full");
        }
    }

    private static Arguments readArguments(String... args) {
        return FilesStatusReport.readArguments(CommandArgumentReader.parse(FilesStatusReport.USAGE, args));
    }
}
