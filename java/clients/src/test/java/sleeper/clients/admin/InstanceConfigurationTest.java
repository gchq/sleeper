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
import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.noChanges;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.withChanges;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class InstanceConfigurationTest extends AdminClientMockStoreBase {

    @Test
    void shouldViewInstanceConfiguration() throws IOException, InterruptedException {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);
        in.enterNextPrompts(INSTANCE_CONFIGURATION_OPTION, EXIT_OPTION);
        when(editor.openPropertiesFile(properties))
                .thenReturn(noChanges(properties));

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + MAIN_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, editor);
        order.verify(in.mock).promptLine(any());
        order.verify(editor).openPropertiesFile(properties);
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    // TODO apply changes in the store, handle multiple properties changed, output description
    // TODO prompt to return to main menu or apply changes
    @Test
    void shouldEditAProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        setInstanceProperties(before);
        before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
        InstanceProperties after = createValidInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "sleeper.s3.max-connections\n" +
                "Before: 123\n" +
                "After: 456\n" +
                "\n");
    }

    @Test
    void shouldSetADefaultedProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        setInstanceProperties(before);
        InstanceProperties after = createValidInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "123");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "sleeper.s3.max-connections\n" +
                "Unset before, default value: 25\n" +
                "After: 123\n" +
                "\n");
    }

    @Test
    void shouldSetAnUnknownProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        setInstanceProperties(before);
        InstanceProperties after = createValidInstanceProperties();
        after.loadFromString("unknown.property=abc");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "unknown.property\n" +
                "Unset before\n" +
                "After: abc\n" +
                "\n");
    }

    private String updatePropertiesGetOutput(InstanceProperties before, InstanceProperties after) throws Exception {
        setInstanceProperties(before);
        in.enterNextPrompts(INSTANCE_CONFIGURATION_OPTION, EXIT_OPTION);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));

        return getScreenOutput(runClientGetOutput());
    }

    private static String getScreenOutput(String output) {
        return output.substring(
                (CLEAR_CONSOLE + MAIN_SCREEN).length(),
                output.length() - (CLEAR_CONSOLE + MAIN_SCREEN).length());
    }

}
