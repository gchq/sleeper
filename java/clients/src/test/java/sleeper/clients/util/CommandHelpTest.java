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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.clients.deploy.DeployExistingInstance;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.deploy.UploadArtefacts;
import sleeper.clients.deploy.container.BuildDockerImage;
import sleeper.clients.report.FilesStatusReport;
import sleeper.clients.table.AddTableClient;
import sleeper.core.util.cli.CommandLineUsage;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CommandHelpTest {
    private static final Pattern OPTION = Pattern.compile("--([a-z-]+)");
    private static final Pattern HELP_HEADING = Pattern.compile("^--([a-z-]+)(?=[ ,\\n])", Pattern.MULTILINE);

    @ParameterizedTest(name = "{0} lists options alphabetically")
    @MethodSource("commands")
    void shouldListOptionsInAlphabeticalOrder(String command, CommandLineUsage usage) {
        String usageMessage = usage.createUsageMessage();
        String availableOptions = usageMessage.substring(usageMessage.indexOf("Available options:"));
        List<String> options = OPTION.matcher(availableOptions).results()
                .map(match -> match.group(1))
                .filter(option -> !"help".equals(option))
                .toList();

        assertAlphabetical(command, options);
    }

    @ParameterizedTest(name = "{0} describes every option alphabetically")
    @MethodSource("commands")
    void shouldDescribeOptionsInAlphabeticalOrder(String command, CommandLineUsage usage) {
        List<String> headings = HELP_HEADING.matcher(usage.createHelpText()).results()
                .map(match -> match.group(1))
                .toList();

        assertAlphabetical(command, headings);
    }

    private static void assertAlphabetical(String command, List<String> options) {
        assertThat(options).as(command).containsExactlyElementsOf(options.stream().sorted().toList());
    }

    private static Stream<Arguments> commands() {
        return Stream.of(
                Arguments.of("addTable", AddTableClient.USAGE),
                Arguments.of("filesStatusReport", FilesStatusReport.USAGE),
                Arguments.of("deployExisting", DeployExistingInstance.USAGE),
                Arguments.of("deployNew", DeployNewInstance.USAGE),
                Arguments.of("uploadArtefacts", UploadArtefacts.USAGE),
                Arguments.of("buildDockerImage", BuildDockerImage.USAGE));
    }
}
