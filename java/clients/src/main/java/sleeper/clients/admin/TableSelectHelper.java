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

package sleeper.clients.admin;

import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.UserExitedException;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.ConsoleChoice;

import static sleeper.clients.admin.AdminCommonPrompts.RETURN_TO_MAIN_MENU;

class TableSelectHelper {
    private final ConsoleOutput out;
    private final ChooseOne chooseOne;

    TableSelectHelper(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.chooseOne = new ChooseOne(out, in);
    }

    public Chosen<ConsoleChoice> chooseTable() throws UserExitedException {
        return chooseTable("")
                .chooseUntilSomethingEntered(() ->
                        chooseTable("\nYou did not enter anything please try again\n"));
    }

    private Chosen<ConsoleChoice> chooseTable(String message) {
        out.clearScreen(message);
        out.println("Which TABLE do you want to select?\n");
        return chooseOne.chooseWithMessageFrom(
                "Please enter the TABLE NAME now or use the following options:",
                RETURN_TO_MAIN_MENU);
    }
}
