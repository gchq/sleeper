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

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

class CommandHelpTest {

    @ParameterizedTest(name = "{0} lists options alphabetically")
    @MethodSource("commands")
    void shouldListOptionsInAlphabeticalOrder(String command, CommandLineUsage usage, List<String> options) {
        assertThat(usage.createUsageMessage())
                .as(command)
                .endsWith("Available options: --help, " + options.stream().map(name -> "--" + name).collect(joining(", ")));
    }

    @ParameterizedTest(name = "{0} describes every option alphabetically")
    @MethodSource("commands")
    void shouldDescribeOptionsInAlphabeticalOrder(String command, CommandLineUsage usage, List<String> options) {
        List<String> headings = Pattern.compile("^--([a-z-]+)(?=[ ,\\n])", Pattern.MULTILINE)
                .matcher(usage.createHelpText())
                .results()
                .map(match -> match.group(1))
                .toList();
        assertThat(headings).as(command).containsExactlyElementsOf(options);
    }

    private static Stream<Arguments> commands() {
        return Stream.of(
                Arguments.of("addTable", AddTableClient.USAGE,
                        List.of("config-dir", "schema", "table-name", "table-properties")),
                Arguments.of("filesStatusReport", FilesStatusReport.USAGE,
                        List.of("max-no-ref-files", "report-type", "verbose")),
                Arguments.of("deployExisting", DeployExistingInstance.USAGE,
                        List.of("force-cdk-app", "paused")),
                Arguments.of("deployNew", DeployNewInstance.USAGE,
                        List.of("config-dir", "paused", "properties-file")),
                Arguments.of("uploadArtefacts", UploadArtefacts.USAGE,
                        List.of("base-image-registry", "cdk-app", "create-builder", "create-deployment", "id", "properties", "upload")),
                Arguments.of("buildDockerImage", BuildDockerImage.USAGE,
                        List.of("default-base-image", "multiplatform")));
    }
}
