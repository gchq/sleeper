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
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
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

    // TODO apply changes in the store, handle multiple properties changed
    // TODO prompt to return to main menu or apply changes
    @Test
    void shouldEditAProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
        InstanceProperties after = createValidInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "sleeper.s3.max-connections\n" +
                "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                "Before: 123\n" +
                "After: 456\n" +
                "\n");
    }

    @Test
    void shouldSetADefaultedProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        InstanceProperties after = createValidInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "123");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "sleeper.s3.max-connections\n" +
                "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                "Unset before, default value: 25\n" +
                "After: 123\n" +
                "\n");
    }

    @Test
    void shouldSetAnUnknownProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        InstanceProperties after = createValidInstanceProperties();
        after.loadFromString("unknown.property=abc");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "unknown.property\n" +
                "Unknown property, no description available\n" +
                "Unset before\n" +
                "After: abc\n" +
                "\n");
    }

    @Test
    void shouldEditPropertyWithLongDescription() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        InstanceProperties after = createValidInstanceProperties();
        after.set(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, "123");

        // When
        String output = updatePropertiesGetOutput(before, after);

        // Then
        assertThat(output).isEqualTo("Found changes to properties:\n" +
                "\n" +
                "sleeper.ingest.partition.refresh.period\n" +
                "The frequency in seconds with which ingest tasks refresh their view of the partitions.\n" +
                "(NB Refreshes only happen once a batch of data has been written so this is a lower bound on the\n" +
                "refresh frequency.)\n" +
                "Unset before, default value: 120\n" +
                "After: 123\n" +
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
