/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.admin;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_PROPERTY_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class InstancePropertyReportTest extends AdminClientMockStoreBase {

    @Test
    void shouldPrintInstancePropertyReportWhenChosen() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Instance Property Report")
                // Then check all the user defined properties are present in the output
                .contains(Stream.of(UserDefinedInstanceProperty.values())
                        .map(UserDefinedInstanceProperty::getPropertyName)
                        .collect(Collectors.toList()))
                // Then check at least one system-defined property is present in the output
                .containsAnyOf(Stream.of(SystemDefinedInstanceProperty.values())
                        .map(SystemDefinedInstanceProperty::getPropertyName)
                        .toArray(String[]::new))
                // Then check some set property values and their descriptions are present in the output
                .contains("# The AWS account number. This is the AWS account that the instance will be deployed to\n" +
                        "sleeper.account: 1234567890\n")
                .contains("# The length of time in days that CloudWatch logs are retained\n" +
                        "sleeper.log.retention.days: 1\n")
                .contains("# A list of tags for the project\n" +
                        "sleeper.tags: name,abc,project,test\n")
                .contains("# The id of the VPC to deploy to\n" +
                        "sleeper.vpc: aVPC\n")
                .contains("# The S3 bucket name used to store configuration files.\n" +
                        "sleeper.config.bucket: sleeper-test-instance-config\n")
                // Then check properties in sequence to check spacing between them
                .contains("# The amount of memory (GB) the athena composite handler has\n" +
                        "sleeper.athena.handler.memory: 4096\n" +
                        "\n" +
                        "# The timeout in seconds for the athena composite handler\n" +
                        "sleeper.athena.handler.timeout.seconds: 900\n" +
                        "\n" +
                        "# The number of days before objects in the spill bucket are deleted.\n" +
                        "sleeper.athena.spill.bucket.ageoff.days: 1")
                // Then check property with multi-line description
                .contains("# A file will not be deleted until this number of seconds have passed after it has been marked as\n" +
                        "# ready for garbage collection. The reason for not deleting files immediately after they have been\n" +
                        "# marked as ready for garbage collection is that they may still be in use by queries. This property\n" +
                        "# can be overridden on a per-table basis.\n" +
                        "sleeper.default.gc.delay.seconds: 600")
                // Then check property with multi-line description and custom line breaks
                .contains("# The minimum number of files to read in a compaction job. Note that the state store must support\n" +
                        "# atomic updates for this many files. For the DynamoDBStateStore this is 11. It can be overridden on a\n" +
                        "# per-table basis.\n" +
                        "# (NB This does not apply to splitting jobs which will run even if there is only 1 file.)\n" +
                        "# This is a default value and will be used if not specified in the table.properties file\n" +
                        "sleeper.default.compaction.files.batch.size: 11");


        // Then check the ordering of some property names are correct
        assertThat(output.indexOf("sleeper.account"))
                .isLessThan(output.indexOf("sleeper.log.retention.days"))
                .isLessThan(output.indexOf("sleeper.vpc"));
        assertThat(output.indexOf("sleeper.log.retention.days"))
                .isLessThan(output.indexOf("sleeper.vpc"));
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
