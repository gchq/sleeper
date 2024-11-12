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

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.clients.util.console.UserExitedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.RETURN_TO_MAIN_SCREEN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

class TableSelectHelperTest extends AdminClientMockStoreBase {
    @Test
    void shouldContinueIfTableExists() {
        // Given
        setTableProperties(TABLE_NAME_VALUE);
        in.enterNextPrompts(TABLE_NAME_VALUE);

        // When
        String output = runTableSelectHelperGetOutput();

        // Then
        assertThat(output)
                .isEqualTo(CLEAR_CONSOLE + TABLE_SELECT_SCREEN + "\n" +
                        "Found table " + TABLE_NAME_VALUE + "\n");
    }

    @Test
    void shouldReturnToMainMenuIfMenuOptionSelected() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(RETURN_TO_MAIN_SCREEN_OPTION);

        // When
        String output = runTableSelectHelperGetOutput();

        // Then
        assertThat(output)
                .isEqualTo(CLEAR_CONSOLE + TABLE_SELECT_SCREEN);
    }

    @Test
    void shouldExitIfMenuOptionSelected() {
        // Given
        setInstanceProperties(createValidInstanceProperties());
        in.enterNextPrompts(EXIT_OPTION);

        // When/Then
        assertThatThrownBy(this::runTableSelectHelperGetOutput)
                .isInstanceOf(UserExitedException.class);
    }

    private String runTableSelectHelperGetOutput() {
        new TableSelectHelper(out.consoleOut(), in.consoleIn(), store)
                .chooseTableOrReturnToMain(instanceId).ifPresent(tableProperties -> out.consoleOut().println("\n" +
                        "Found table " + tableProperties.get(TABLE_NAME)));
        return out.toString();
    }
}
