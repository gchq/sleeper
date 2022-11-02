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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConsoleInput {

    private final ConsolePrompter prompter = mock(ConsolePrompter.class);
    private final ConsoleConfirmer confirmer = mock(ConsoleConfirmer.class);

    public ConsoleInput consoleIn() {
        return new ConsoleInput(prompter, confirmer);
    }

    public void enterNextPrompt(String entered) {
        enterNextPrompts(entered);
    }

    public void enterNextPrompts(String entered, String... otherEntered) {
        when(prompter.promptLine(any())).thenReturn(entered, otherEntered);
    }
}
