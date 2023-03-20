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
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.RETURN_TO_MAIN_SCREEN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_PROPERTY_REPORT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class TablePropertyReportTest extends AdminClientMockStoreBase {
    @Test
    void shouldPrintAllTableProperties() {
        // Given
        createTablePropertiesAndSelectTable();

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output)
                .startsWith(CLEAR_CONSOLE + MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN)
                .endsWith(PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN)
                .contains("Table Property Report")
                .contains(TableProperty.getAll().stream()
                        .map(TableProperty::getPropertyName)
                        .collect(Collectors.toList()));

        confirmAndVerifyNoMoreInteractions();
    }

    @Test
    void shouldExitWhenChosenOnTablePropertyReportScreen() {
        // Given
        createTableProperties();
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + TABLE_SELECT_SCREEN);

        verify(in.mock, times(2)).promptLine(any());
        verifyNoMoreInteractions(in.mock);
    }

    @Test
    void shouldReturnToMainScreenWhenChosenOnTablePropertyReportScreen() {
        // Given
        createTableProperties();
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, RETURN_TO_MAIN_SCREEN_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + TABLE_SELECT_SCREEN
                + CLEAR_CONSOLE + MAIN_SCREEN);

        verify(in.mock, times(3)).promptLine(any());
        verifyNoMoreInteractions(in.mock);
    }

    private void createTablePropertiesAndSelectTable() {
        TableProperties tableProperties = createTableProperties();
        in.enterNextPrompts(TABLE_PROPERTY_REPORT_OPTION, tableProperties.get(TABLE_NAME), EXIT_OPTION);
    }

    private TableProperties createTableProperties() {
        InstanceProperties instanceProperties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(instanceProperties);
        setInstanceProperties(instanceProperties, tableProperties);
        return tableProperties;
    }

    private void confirmAndVerifyNoMoreInteractions() {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(2)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
