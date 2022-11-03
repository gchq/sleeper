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
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_INPUT_NOT_RECOGNISED;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

public class AdminMainScreenTest extends AdminClientMockStoreBase {

    @Test
    public void shouldDisplayMainScreenAndExitWhenChosen() throws Exception {
        // Given
        in.enterNextPrompt(EXIT_OPTION);

        // When / Then
        assertThat(runClientGetOutput()).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN);
    }

    @Test
    public void shouldPromptOnInvalidChoiceOnMainScreen() throws Exception {
        // Given
        in.enterNextPrompts("abc", EXIT_OPTION);

        // When / Then
        assertThat(runClientGetOutput()).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN +
                CLEAR_CONSOLE + PROMPT_INPUT_NOT_RECOGNISED + MAIN_SCREEN);
    }

}
