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

import java.io.Console;

public class ConsoleInput {

    private final ConsolePrompter prompter;
    private final ConsoleConfirmer confirmer;

    public ConsoleInput(Console console) {
        this(console::readLine, console::readLine);
    }

    public ConsoleInput(ConsolePrompter prompter, ConsoleConfirmer confirmer) {
        this.prompter = prompter;
        this.confirmer = confirmer;
    }

    public String promptLine(String prompt) {
        return prompter.promptLine(prompt);
    }

    public void waitForConfirmation() {
        confirmer.waitForConfirmation();
    }
}
