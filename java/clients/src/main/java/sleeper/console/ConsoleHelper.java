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

package sleeper.console;

import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.ConsoleChoice;
import sleeper.console.menu.MenuOption;

import static sleeper.clients.admin.AdminCommonPrompts.RETURN_TO_MAIN_MENU;

public class ConsoleHelper {
    private final ConsoleOutput out;
    private final ChooseOne chooseOne;

    public ConsoleHelper(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.chooseOne = new ChooseOne(out, in);
    }

    public Chosen<ConsoleChoice> getInputStringOrChooseExit(String chooseMessage) throws UserExitedException {
        return getInputStringOrChooseExit("", chooseMessage);
    }

    public Chosen<ConsoleChoice> getInputStringOrChooseExit(String title, String chooseMessage) throws UserExitedException {
        return chooseWithTitleAndMessage("\n" + title, chooseMessage)
                .chooseUntilSomethingEntered(() ->
                        chooseWithTitleAndMessage("\nYou did not enter anything please try again\n" + title,
                                chooseMessage));
    }

    private Chosen<ConsoleChoice> chooseWithTitleAndMessage(String title, String chooseMessage) {
        out.clearScreen(title);
        return chooseOne.chooseWithMessageFrom(
                chooseMessage,
                RETURN_TO_MAIN_MENU);
    }

    public MenuOption chooseOptionUntilValid(String message, MenuOption... options) {
        return chooseOne.chooseWithMessageFrom(message, options)
                .chooseUntilChoiceFound(() -> chooseOne.chooseWithMessageFrom(
                        "\nInput not recognised please try again\n", options));
    }
}
