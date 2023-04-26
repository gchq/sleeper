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
package sleeper.clients.util.console.menu;

import org.junit.jupiter.api.Test;

import sleeper.clients.util.console.UserExitedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ChooseUntilChoiceFoundTest extends ChooseOneTestBase {

    @Test
    public void shouldChooseAgainOnceWhenNothingChosenFirstTime() {
        // Given
        in.enterNextPrompts("123", "1");

        // When / Then
        assertThat(chooseTestOption()
                .chooseUntilChoiceFound(this::chooseTestOption))
                .isEqualTo(OPTION_ONE);
    }

    @Test
    public void shouldChooseAgainTwiceWhenNothingChosenFirstTwoTimes() {
        // Given
        in.enterNextPrompts("123", "456", "2");

        // When / Then
        assertThat(chooseTestOption()
                .chooseUntilChoiceFound(this::chooseTestOption))
                .isEqualTo(OPTION_TWO);
    }

    @Test
    public void shouldExitWhenChoosingAgainWhenNothingChosenFirstTime() {
        // Given
        in.enterNextPrompts("123", "0");

        // When / Then
        Chosen<ConsoleChoice> chosen = chooseTestOption();
        assertThatThrownBy(() ->
                chosen.chooseUntilChoiceFound(this::chooseTestOption))
                .isInstanceOf(UserExitedException.class);
    }

    @Test
    public void shouldExitWhenChoosingAgainWhenNothingChosenFirstTwoTimes() {
        // Given
        in.enterNextPrompts("123", "456", "0");

        // When / Then
        Chosen<ConsoleChoice> chosen = chooseTestOption();
        assertThatThrownBy(() ->
                chosen.chooseUntilChoiceFound(this::chooseTestOption))
                .isInstanceOf(UserExitedException.class);
    }
}
