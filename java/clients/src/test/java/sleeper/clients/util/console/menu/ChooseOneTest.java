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

class ChooseOneTest extends ChooseOneTestBase {

    @Test
    void shouldOutputOptions() {
        // When
        in.enterNextPrompt("");
        chooseTestOption();

        // Then
        assertThat(out).hasToString("" +
                "Please select from the below options and hit return:\n" +
                "[0] Exit program\n" +
                "[1] Option 1\n" +
                "[2] Option 2\n" +
                "\n" +
                "Input: \n");
    }

    @Test
    void shouldOutputSpecifiedMessage() {
        // When
        in.enterNextPrompt("");
        chooseTestOptionWithMessage("Please enter your name or choose an option and hit return:");

        // Then
        assertThat(out).hasToString("" +
                "Please enter your name or choose an option and hit return:\n" +
                "[0] Exit program\n" +
                "[1] Option 1\n" +
                "[2] Option 2\n" +
                "\n" +
                "Input: \n");
    }

    @Test
    void shouldReturnFirstOptionWhenChosen() {
        // Given
        in.enterNextPrompt("1");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption();

        // Then
        assertThat(chosen.getChoice()).containsSame(OPTION_ONE);
        assertThat(chosen.getEntered()).isEqualTo("1");
    }

    @Test
    void shouldReturnSecondOptionWhenChosen() {
        // Given
        in.enterNextPrompt("2");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption();

        // Then
        assertThat(chosen.getChoice()).containsSame(OPTION_TWO);
        assertThat(chosen.getEntered()).isEqualTo("2");
    }

    @Test
    void shouldExitWhenChosen() {
        // Given
        in.enterNextPrompt("0");

        // When / Then
        assertThatThrownBy(this::chooseTestOption)
                .isInstanceOf(UserExitedException.class);
    }

    @Test
    void shouldReturnNoChoiceWhenNoneEntered() {
        // Given
        in.enterNextPrompt("");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption();

        // Then
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEmpty();
    }

    @Test
    void shouldReturnEnteredString() {
        // Given
        in.enterNextPrompt("test value");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption();

        // Then
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("test value");
    }

    @Test
    void shouldReturnNoChoiceWhenEnteredNumberTooLarge() {
        // Given
        in.enterNextPrompt("10");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption();

        // Then
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("10");
    }

    @Test
    void shouldReturnNoChoiceWhenEnteredNumberTooSmall() {
        // Given
        in.enterNextPrompt("-1");

        // When
        Chosen<ConsoleChoice> chosen = chooseTestOption();

        // Then
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("-1");
    }

    @Test
    void shouldChooseAnOptionUsingAnEnum() {
        // Given
        in.enterNextPrompt("1");

        // When
        Chosen<TestOption> chosen = chooseOne().chooseFrom(TestOption.values());

        // Then
        assertThat(chosen.getChoice()).containsSame(TestOption.ONE);
        assertThat(chosen.getEntered()).isEqualTo("1");
    }
}
