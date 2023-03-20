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

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

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
    void shouldReturnToMenuIfTableDoesNotExist() {
        // Given
        in.enterNextPrompts("unknown-table");

        // When
        String output = runTableSelectHelperGetOutput();

        // Then
        assertThat(output)
                .isEqualTo(CLEAR_CONSOLE + TABLE_SELECT_SCREEN + "\n" +
                        "Error: Properties for table \"unknown-table\" could not be found" +
                        PROMPT_RETURN_TO_MAIN);
    }

    private String runTableSelectHelperGetOutput() {
        new TableSelectHelper(out.consoleOut(), in.consoleIn(), store)
                .chooseTableIfExistsThen(INSTANCE_ID, tableProperties ->
                        out.consoleOut().println("Found table " + tableProperties.get(TABLE_NAME)));
        return out.toString();
    }
}
