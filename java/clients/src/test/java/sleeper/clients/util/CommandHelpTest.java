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
package sleeper.clients.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.clients.deploy.DeployExistingInstance;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.deploy.UploadArtefacts;
import sleeper.clients.deploy.container.BuildDockerImage;
import sleeper.clients.report.FilesStatusReport;
import sleeper.clients.report.IngestJobStatusReport;
import sleeper.clients.table.AddTableClient;
import sleeper.core.util.cli.CommandLineUsage;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CommandHelpTest {
    private static final Pattern OPTION = Pattern.compile("--([^\\s,]+)");
    private static final Pattern HELP_HEADING = Pattern.compile("^--([^\\s,]+)(?=[\\s,]|$)", Pattern.MULTILINE);

    @ParameterizedTest(name = "{0} lists options in usage message alphabetically")
    @MethodSource("commands")
    void shouldListOptionsInUsageMessageInAlphabeticalOrder(String command, CommandLineUsage usage) {
        List<String> options = getOrderOfOptionNamesInUsageMessage(usage.createUsageMessage());

        assertThat(options).as(command).containsExactlyElementsOf(alphabeticalOrder(options));
    }

    @ParameterizedTest(name = "{0} describes every option in help text alphabetically")
    @MethodSource("commands")
    void shouldDescribeEveryOptionInHelpTextInAlphabeticalOrder(String command, CommandLineUsage usage) {
        List<String> optionNames = getOrderOfOptionNamesInUsageMessage(usage.createUsageMessage());
        List<String> headings = getOrderOfOptionHeadingsInHelpText(usage.createHelpText());

        assertThat(headings).as(command).containsExactlyElementsOf(optionNames);
        assertThat(headings).as(command).containsExactlyElementsOf(alphabeticalOrder(headings));
    }

    @Test
    void shouldReadOrderOfOptionNamesInUsageMessage() {
        String usageMessage = "Usage: <arg>\nAvailable options: --help, --Upper_Case, --dot.name, --slash/value";

        assertThat(getOrderOfOptionNamesInUsageMessage(usageMessage))
                .containsExactly("Upper_Case", "dot.name", "slash/value");
    }

    @Test
    void shouldReadOrderOfOptionHeadingsInHelpText() {
        String helpText = """
                Available options: --help, --Upper_Case, --dot.name, --slash/value

                --Upper_Case First option
                --dot.name, -d Second option
                --slash/value
                Third option
                """;

        assertThat(getOrderOfOptionHeadingsInHelpText(helpText))
                .containsExactly("Upper_Case", "dot.name", "slash/value");
    }

    private static List<String> getOrderOfOptionNamesInUsageMessage(String usageMessage) {
        String availableOptions = usageMessage.substring(usageMessage.indexOf("Available options:"));
        return OPTION.matcher(availableOptions).results()
                .map(match -> match.group(1))
                .filter(option -> !"help".equals(option))
                .toList();
    }

    private static List<String> getOrderOfOptionHeadingsInHelpText(String helpText) {
        return HELP_HEADING.matcher(helpText).results()
                .map(match -> match.group(1))
                .toList();
    }

    private static List<String> alphabeticalOrder(List<String> options) {
        return options.stream().sorted().toList();
    }

    private static Stream<Arguments> commands() {
        return Stream.of(
                Arguments.of("addTable", AddTableClient.USAGE),
                Arguments.of("filesStatusReport", FilesStatusReport.USAGE),
                Arguments.of("ingestJobStatusReport", IngestJobStatusReport.USAGE),
                Arguments.of("deployExisting", DeployExistingInstance.USAGE),
                Arguments.of("deployNew", DeployNewInstance.USAGE),
                Arguments.of("uploadArtefacts", UploadArtefacts.USAGE),
                Arguments.of("buildDockerImage", BuildDockerImage.USAGE));
    }
}
