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

public class ChooseUntilSomethingEnteredTest extends ChooseOneTestBase {

    @Test
    public void shouldChooseAgainOnceWhenNothingEnteredFirstTime() {
        // Given
        in.enterNextPrompts("", "1");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption()
                .chooseUntilSomethingEntered(this::chooseTestOption);

        // Then
        assertThat(chosen.getChoice()).containsSame(OPTION_ONE);
    }

    @Test
    public void shouldChooseAgainTwiceWhenNothingEnteredFirstTwoTimes() {
        // Given
        in.enterNextPrompts("", "", "2");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption()
                .chooseUntilSomethingEntered(this::chooseTestOption);

        // Then
        assertThat(chosen.getChoice()).containsSame(OPTION_TWO);
    }

    @Test
    public void shouldEnterValueWhenNothingEnteredFirstTime() {
        // Given
        in.enterNextPrompts("", "second entry");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption()
                .chooseUntilSomethingEntered(this::chooseTestOption);

        // Then
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("second entry");
    }

    @Test
    public void shouldEnterValueWhenNothingEnteredFirstTwoTimes() {
        // Given
        in.enterNextPrompts("", "", "third entry");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption()
                .chooseUntilSomethingEntered(this::chooseTestOption);

        // Then
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("third entry");
    }

    @Test
    public void shouldExitWhenChoosingAgainWhenNothingEnteredFirstTime() {
        // Given
        in.enterNextPrompts("", "0");

        // When / Then
        Chosen<ConsoleChoice> chosen = chooseTestOption();
        assertThatThrownBy(() ->
                chosen.chooseUntilSomethingEntered(this::chooseTestOption))
                .isInstanceOf(UserExitedException.class);
    }

    @Test
    public void shouldExitWhenChoosingAgainWhenNothingEnteredFirstTwoTimes() {
        // Given
        in.enterNextPrompts("", "", "0");

        // When / Then
        Chosen<ConsoleChoice> chosen = chooseTestOption();
        assertThatThrownBy(() ->
                chosen.chooseUntilSomethingEntered(this::chooseTestOption))
                .isInstanceOf(UserExitedException.class);
    }
}
