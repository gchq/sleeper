/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import sleeper.clients.admin.testutils.AdminClientMockStoreBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.RETURN_TO_MAIN_SCREEN_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_ENTER_TABLE_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_ENTER_VALUE_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.UPDATE_PROPERTY_SCREEN;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

public class UpdatePropertyReturnToMainTest extends AdminClientMockStoreBase {

    @Test
    public void shouldReturnToMainScreenWhenChosenOnPropertyNameInput() {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, RETURN_TO_MAIN_SCREEN_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + MAIN_SCREEN);
    }

    @Test
    public void shouldReturnToMainScreenWhenChosenOnPropertyValueInput() {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.retain.infra.after.destroy",
                RETURN_TO_MAIN_SCREEN_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + CLEAR_CONSOLE + MAIN_SCREEN);
    }

    @Test
    public void shouldReturnToMainScreenWhenChosenOnTableNameInput() {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.table.iterator.class.name", "SomeIteratorClass",
                RETURN_TO_MAIN_SCREEN_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_TABLE_SCREEN
                + CLEAR_CONSOLE + MAIN_SCREEN);
    }
}
