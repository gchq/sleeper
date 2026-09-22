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

import org.junit.jupiter.api.Test;

import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReportTypeArgumentTest {

    private final ReportTypeArgument<String> reportType = ReportTypeArgument
            .withDefault("STANDARD", "standard-reporter")
            .addReporter("JSON", "json-reporter")
            .addReporter("CSV", "csv-reporter")
            .build();

    private final CommandLineUsage usage = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id"))
            .options(List.of(ReportTypeArgument.option()))
            .build();

    @Test
    void shouldReadDefaultReporterWhenOptionNotSet() {
        // When / Then
        assertThat(read("my-instance")).isEqualTo("standard-reporter");
    }

    @Test
    void shouldReadReporterWhenOptionSet() {
        // When / Then
        assertThat(read("my-instance", "--report-type", "JSON")).isEqualTo("json-reporter");
    }

    @Test
    void shouldReadReporterIgnoringCase() {
        // When / Then
        assertThat(read("my-instance", "--report-type", "json")).isEqualTo("json-reporter");
    }

    @Test
    void shouldFailWhenReportTypeIsNotSupported() {
        // When / Then
        assertThatThrownBy(() -> read("my-instance", "--report-type", "xml"))
                .isInstanceOf(CommandArgumentsException.class)
                .hasMessage("Report type not supported: xml. Valid types: STANDARD, JSON, CSV");
    }

    @Test
    void shouldDeclareOptionWithSharedName() {
        // When
        CommandOption option = ReportTypeArgument.option();

        // Then
        assertThat(option.longName()).isEqualTo("report-type");
        assertThat(option.shortName()).isNull();
        assertThat(option.isFlag()).isFalse();
    }

    @Test
    void shouldCreateHelpTextListingTypesWithTheDefaultFirst() {
        // When / Then
        assertThat(reportType.helpText()).isEqualTo("" +
                "--report-type <type>\n" +
                "Output format. One of STANDARD, JSON, CSV. Defaults to STANDARD.");
    }

    private String read(String... args) {
        CommandArguments arguments = CommandArgumentReader.parse(usage, args);
        return reportType.read(arguments);
    }
}
