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
package sleeper.clients.testutil;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConsoleInput {

    public static final String CONFIRM_PROMPT = "";

    public final ConsoleInput mock = mock(ConsoleInput.class);
    private final Queue<String> nextPrompts = new LinkedList<>();

    public TestConsoleInput(ConsoleOutput out) {
        when(mock.promptLine(any())).thenAnswer(invocation -> {
            String prompt = invocation.getArgument(0);
            out.println(prompt);
            if (nextPrompts.isEmpty()) {
                throw new IllegalStateException("User was prompted when all specified prompts were exhausted");
            }
            return nextPrompts.poll();
        });
        doAnswer(invocation -> {
            if (!CONFIRM_PROMPT.equals(nextPrompts.poll())) {
                throw new IllegalStateException("User was prompted to continue with no confirmation specified, remaining specified prompts: " + nextPrompts);
            }
            return null;
        }).when(mock).waitForLine();
    }

    public ConsoleInput consoleIn() {
        return mock;
    }

    public void enterNextPrompt(String entered) {
        enterNextPrompts(entered);
    }

    public void enterNextPrompts(String... entered) {
        nextPrompts.addAll(Arrays.asList(entered));
    }
}
