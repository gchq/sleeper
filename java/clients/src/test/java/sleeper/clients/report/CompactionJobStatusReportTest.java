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

import sleeper.clients.report.CompactionJobStatusReport.Arguments;
import sleeper.clients.report.compaction.job.JsonCompactionJobStatusReporter;
import sleeper.clients.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.util.cli.CommandArgumentReader;

import java.time.Instant;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobStatusReportTest {

    private final ToStringConsoleOutput output = new ToStringConsoleOutput();
    private final TestConsoleInput input = new TestConsoleInput(output.consoleOut());

    @Nested
    class ParseArguments {

        @Test
        void shouldReadDefaultsWhenOnlyRequiredArgsGiven() {
            // When
            Arguments args = readArguments("my-instance", "my-table", "--all");

            // Then
            assertThat(args.instanceId()).isEqualTo("my-instance");
            assertThat(args.tableName()).isEqualTo("my-table");
            assertThat(args.reporter()).isInstanceOf(StandardCompactionJobStatusReporter.class);
        }

        @Test
        void shouldReadReportTypeJson() {
            // When
            Arguments args = readArguments("json-instance", "json-table", "--format", "json", "--all");

            // Then
            assertThat(args.reporter()).isInstanceOf(JsonCompactionJobStatusReporter.class);
        }
    }

    private Arguments readArguments(String... args) {
        return readArguments(() -> {
            throw new IllegalStateException("Unexpected time query");
        }, args);
    }

    private Arguments readArguments(Supplier<Instant> timeSupplier, String... args) {
        return CompactionJobStatusReport.readArguments(
                CommandArgumentReader.parse(IngestJobStatusReport.USAGE, args),
                timeSupplier, input.consoleIn());
    }
}
