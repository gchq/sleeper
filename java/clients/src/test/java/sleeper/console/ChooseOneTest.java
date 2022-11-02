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
package sleeper.console;

import org.junit.Test;
import sleeper.ToStringPrintStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ChooseOneTest {

    private final ToStringPrintStream out = new ToStringPrintStream();
    private final TestConsoleInput in = new TestConsoleInput();

    @Test
    public void shouldOutputOptions() throws Exception {
        // When
        chooseTestOption();

        // Then
        assertThat(out).hasToString("" +
                "Please select from the below options and hit return:\n" +
                "[0] Exit program\n" +
                "[1] Option 1\n" +
                "[2] Option 2\n" +
                "\n");
    }

    @Test
    public void shouldOutputSpecifiedMessage() throws Exception {
        // When
        chooseTestOptionWithMessage("Please enter your name or choose an option and hit return:");

        // Then
        assertThat(out).hasToString("" +
                "Please enter your name or choose an option and hit return:\n" +
                "[0] Exit program\n" +
                "[1] Option 1\n" +
                "[2] Option 2\n" +
                "\n");
    }

    @Test
    public void shouldReturnFirstOptionWhenChosen() throws Exception {
        // Given
        in.enterNextPrompt("1");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isFalse();
        assertThat(chosen.getChoice()).containsSame(TestOption.ONE);
        assertThat(chosen.getEntered()).isEqualTo("1");
    }

    @Test
    public void shouldReturnSecondOptionWhenChosen() throws Exception {
        // Given
        in.enterNextPrompt("2");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isFalse();
        assertThat(chosen.getChoice()).containsSame(TestOption.TWO);
        assertThat(chosen.getEntered()).isEqualTo("2");
    }

    @Test
    public void shouldExitWhenChosen() throws Exception {
        // Given
        in.enterNextPrompt("0");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isTrue();
        assertThat(chosen.getEntered()).isEqualTo("0");
    }

    @Test
    public void shouldReturnNoChoiceWhenNoneEntered() throws Exception {
        // Given
        in.enterNextPrompt("");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isFalse();
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEmpty();
    }

    @Test
    public void shouldReturnEnteredString() throws Exception {
        // Given
        in.enterNextPrompt("test value");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isFalse();
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("test value");
    }

    @Test
    public void shouldReturnNoChoiceWhenEnteredNumberTooLarge() throws Exception {
        // Given
        in.enterNextPrompt("10");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isFalse();
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("10");
    }

    @Test
    public void shouldReturnNoChoiceWhenEnteredNumberTooSmall() throws Exception {
        // Given
        in.enterNextPrompt("-1");

        // When
        Chosen<TestOption> chosen = chooseTestOption();

        // Then
        assertThat(chosen.isExit()).isFalse();
        assertThat(chosen.getChoice()).isEmpty();
        assertThat(chosen.getEntered()).isEqualTo("-1");
    }

    private Chosen<TestOption> chooseTestOption() throws Exception {
        return new ChooseOne(out.consoleOut(), in.consoleIn())
                .chooseFrom(TestOption.values());
    }

    private Chosen<TestOption> chooseTestOptionWithMessage(String message) throws Exception {
        return new ChooseOne(out.consoleOut(), in.consoleIn())
                .chooseWithMessageFrom(message, TestOption.values());
    }

    private enum TestOption implements ConsoleChoice {
        ONE("Option 1"),
        TWO("Option 2");

        private final String description;

        TestOption(String description) {
            this.description = description;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
