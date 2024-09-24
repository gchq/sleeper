/*
 * Copyright 2022-2024 Crown Copyright
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
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_NAMES_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class AdminClientIT extends AdminClientITBase {

    @Test
    void shouldViewInstancePropertiesWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();

        // When
        String output = viewInstanceConfiguration(instanceProperties).exitGetOutput();

        // Then
        assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN + DISPLAY_MAIN_SCREEN);
        verify(editor).openPropertiesFile(instanceProperties);
    }

    @Test
    void shouldPrintTableNamesReportWhenChosen() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        TableProperties tableProperties1 = createValidTableProperties(instanceProperties, "test-table-1");
        TableProperties tableProperties2 = createValidTableProperties(instanceProperties, "test-table-2");
        setInstanceProperties(instanceProperties, tableProperties1, tableProperties2);

        // When
        String output = runClient()
                .enterPrompts(TABLE_NAMES_REPORT_OPTION, CONFIRM_PROMPT, EXIT_OPTION)
                .exitGetOutput();

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
        TableProperties tableProperties = createValidTableProperties(instanceProperties);

        // When
        String output = viewTableConfiguration(instanceProperties, tableProperties).exitGetOutput();

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
        InstanceProperties after = InstanceProperties.copyOf(before);
        after.set(MAXIMUM_CONNECTIONS_TO_S3, "2");

        // When
        String output = editInstanceConfiguration(before, after)
                .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                        PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                        DISPLAY_MAIN_SCREEN);

        InstanceProperties found = S3InstanceProperties.loadFromBucket(s3, before.get(CONFIG_BUCKET));
        assertThat(found.getInt(MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo(2);
    }

    @Test
    void shouldEditATableProperty() throws Exception {
        // Given
        InstanceProperties instanceProperties = createValidInstanceProperties();
        TableProperties before = createValidTableProperties(instanceProperties);
        before.set(ITERATOR_CLASS_NAME, "BeforeIteratorClass");
        TableProperties after = TableProperties.copyOf(before);
        after.set(ITERATOR_CLASS_NAME, "AfterIteratorClass");

        // When
        String output = editTableConfiguration(instanceProperties, before, after)
                .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then
        assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                        PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                        DISPLAY_MAIN_SCREEN);

        TableProperties found = tablePropertiesStore.loadByName(before.get(TABLE_NAME));
        assertThat(found.get(ITERATOR_CLASS_NAME)).isEqualTo("AfterIteratorClass");
    }

    @Test
    void shouldFailAtStartupWhenInstanceDoesNotExist() throws Exception {
        // When
        instanceId = "not-an-instance";
        String output = runClient().runGetOutput();

        // Then
        assertThat(output).startsWith("Could not load properties for instance not-an-instance\n" +
                "Cause: The specified bucket does not exist")
                .contains("Amazon S3");
    }
}
