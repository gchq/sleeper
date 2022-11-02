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

import java.io.UnsupportedEncodingException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ChooseOneTest {

    private final ToStringPrintStream out = new ToStringPrintStream();
    private final TestConsoleInput in = new TestConsoleInput();

    @Test
    public void shouldOutputOptions() throws Exception {
        chooseTestOption();
        assertThat(out).hasToString("" +
                "Please select from the below options and hit return:\n" +
                "[0] Exit program\n" +
                "[1] Option 1\n" +
                "[2] Option 2\n" +
                "\n");
    }

    @Test
    public void shouldReturnFirstOptionWhenChosen() throws Exception {
        in.enterNextPrompt("1");
        assertThat(chooseTestOption()).containsSame(TestOption.ONE);
    }

    @Test
    public void shouldReturnSecondOptionWhenChosen() throws Exception {
        in.enterNextPrompt("2");
        assertThat(chooseTestOption()).containsSame(TestOption.TWO);
    }

    @Test
    public void shouldExitAndCleanupWhenChosen() throws Exception {
        Runnable exit = mock(Runnable.class);
        in.enterNextPrompt("0");
        assertThat(chooseTestOptionWithExitFn(exit)).isEmpty();
        verify(exit).run();
    }

    @Test
    public void shouldReturnNoChoiceWhenNoneEntered() throws Exception {
        in.enterNextPrompt("");
        assertThat(chooseTestOption()).isEmpty();
    }

    @Test
    public void shouldReturnNoChoiceWhenEnteredNumberTooLarge() throws Exception {
        in.enterNextPrompt("10");
        assertThat(chooseTestOption()).isEmpty();
    }

    @Test
    public void shouldReturnNoChoiceWhenEnteredNumberTooSmall() throws Exception {
        in.enterNextPrompt("-1");
        assertThat(chooseTestOption()).isEmpty();
    }

    private Optional<ChooseOne.Choice> chooseTestOption() throws UnsupportedEncodingException {
        return chooseTestOptionWithExitFn(() -> {
            throw new IllegalStateException("Unexpected exit");
        });
    }

    private Optional<ChooseOne.Choice> chooseTestOptionWithExitFn(Runnable exitFn) throws UnsupportedEncodingException {
        return new ChooseOne(out.consoleOut(), in.consoleIn(), exitFn).chooseFrom(TestOption.values());
    }

    private enum TestOption implements ChooseOne.Choice {
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
