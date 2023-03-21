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

import sleeper.clients.admin.testutils.AdminClientITBase;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.noChanges;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.withChanges;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.NO_INSTANCE_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_NAMES_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.console.TestConsoleInput.CONFIRM_PROMPT;

class AdminClientIT extends AdminClientITBase {

    @Test
    void shouldViewInstancePropertiesWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);

        in.enterNextPrompts(INSTANCE_CONFIGURATION_OPTION, EXIT_OPTION);
        when(editor.openPropertiesFile(instanceProperties)).thenReturn(noChanges(instanceProperties));

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN + DISPLAY_MAIN_SCREEN);
        verify(editor).openPropertiesFile(instanceProperties);
    }

    @Test
    void shouldPrintTableNamesReportWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);
        TableProperties tableProperties1 = createValidTableProperties(instanceProperties, "test-table-1");
        tableProperties1.saveToS3(s3);
        TableProperties tableProperties2 = createValidTableProperties(instanceProperties, "test-table-2");
        tableProperties2.saveToS3(s3);
        in.enterNextPrompts(TABLE_NAMES_REPORT_OPTION, CONFIRM_PROMPT, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN + "\n\n" +
                "Table Names\n" +
                "----------------------------------\n" +
                "test-table-1\n" +
                "test-table-2\n" +
                PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);
    }

    @Test
    void shouldViewTablePropertiesWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        tableProperties.saveToS3(s3);

        in.enterNextPrompts(TABLE_CONFIGURATION_OPTION, tableProperties.get(TABLE_NAME), EXIT_OPTION);
        when(editor.openPropertiesFile(tableProperties)).thenReturn(noChanges(tableProperties));

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN +
                CLEAR_CONSOLE + TABLE_SELECT_SCREEN +
                CLEAR_CONSOLE + MAIN_SCREEN);
        verify(editor).openPropertiesFile(tableProperties);
    }

    @Test
    void shouldEditAnInstanceProperty() throws Exception {
        // Given
        InstanceProperties before = createValidInstanceProperties();
        before.saveToS3(s3);
        InstanceProperties after = createValidInstanceProperties();
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "2");

        in.enterNextPrompts(INSTANCE_CONFIGURATION_OPTION, SaveChangesScreen.SAVE_CHANGES_OPTION,
                CONFIRM_PROMPT, EXIT_OPTION);
        when(editor.openPropertiesFile(before)).thenReturn(withChanges(before, after));

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                        PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                        DISPLAY_MAIN_SCREEN);

        InstanceProperties found = new InstanceProperties();
        found.loadFromS3(s3, before.get(CONFIG_BUCKET));
        assertThat(found.getInt(MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo(2);
    }

    @Test
    void shouldEditATableProperty() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        instanceProperties.saveToS3(s3);
        TableProperties before = createValidTableProperties(instanceProperties);
        before.set(ITERATOR_CLASS_NAME, "BeforeIteratorClass");
        before.saveToS3(s3);
        TableProperties after = createValidTableProperties(instanceProperties);
        after.set(ITERATOR_CLASS_NAME, "AfterIteratorClass");

        in.enterNextPrompts(TABLE_CONFIGURATION_OPTION, TABLE_NAME_VALUE,
                SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT, EXIT_OPTION);
        when(editor.openPropertiesFile(before)).thenReturn(withChanges(before, after));

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                        PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                        DISPLAY_MAIN_SCREEN);

        TableProperties found = new TableProperties(instanceProperties);
        found.loadFromS3(s3, before.get(TABLE_NAME));
        assertThat(found.get(ITERATOR_CLASS_NAME)).isEqualTo("AfterIteratorClass");
    }

    @Test
    void shouldFailAtStartupWhenInstanceDoesNotExist() throws Exception {

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).startsWith(NO_INSTANCE_SCREEN +
                        "Cause: The specified key does not exist.")
                .contains("Amazon S3");
    }
}
