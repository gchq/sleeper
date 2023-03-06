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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_PROPERTY_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class InstancePropertyReportTest extends AdminClientMockStoreBase {

    @Test
    void shouldPrintAllInstanceProperties() {
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
                // Check all the user defined properties are present in the output
                .contains(UserDefinedInstanceProperty.getAll().stream()
                        .map(UserDefinedInstanceProperty::getPropertyName)
                        .collect(Collectors.toList()))
                // Check at least one system-defined property is present in the output
                .containsAnyOf(SystemDefinedInstanceProperty.getAll().stream()
                        .map(SystemDefinedInstanceProperty::getPropertyName)
                        .toArray(String[]::new));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertyGroupDescriptions() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .contains("# The following properties are commonly used throughout Sleeper\n\n")
                .contains("# The following properties relate to standard ingest\n\n")
                .contains("# The following properties relate to bulk import, i.e. ingesting data using Spark jobs running on EMR\n" +
                        "# or EKS.\n\n")
                .contains("# The following properties relate to the splitting of partitions\n\n")
                .contains("# The following properties relate to compactions.\n\n")
                .contains("# The following properties relate to queries.\n\n");

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertiesInTheCorrectOrder() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output.indexOf("sleeper.account"))
                .isLessThan(output.indexOf("sleeper.log.retention.days"))
                .isLessThan(output.indexOf("sleeper.vpc"));
        assertThat(output.indexOf("sleeper.ingest"))
                .isLessThan(output.indexOf("sleeper.compaction"));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldPrintPropertyGroupsInTheCorrectOrder() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output.indexOf("The following properties relate to standard ingest"))
                .isLessThan(output.indexOf("The following properties relate to bulk import"));
        assertThat(output.indexOf("The following properties relate to garbage collection"))
                .isLessThan(output.indexOf("The following properties relate to compactions"));
        assertThat(output.indexOf("The following properties relate to compactions"))
                .isLessThan(output.indexOf("The following properties relate to queries"));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldDisplayPropertiesInTheCorrectGroup() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(INSTANCE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then check that one UserDefinedInstanceProperty is in the correct group
        assertThat(output.indexOf("sleeper.id"))
                .isBetween(
                        output.indexOf("The following properties are commonly used throughout Sleeper"),
                        output.indexOf("The following properties relate to standard ingest"));
        // Then check that one SystemDefinedInstanceProperty is in the correct group
        assertThat(output.indexOf("sleeper.config.bucket"))
                .isBetween(
                        output.indexOf("The following properties are commonly used throughout Sleeper"),
                        output.indexOf("The following properties relate to standard ingest"));

        confirmAndVerifyNoMoreInteractions();
    }

    private void confirmAndVerifyNoMoreInteractions() {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
