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

package sleeper.clients.util.console;

import sleeper.clients.util.console.menu.ChooseOne;
import sleeper.clients.util.console.menu.MenuOption;

public class ConsoleHelper {
    private final ChooseOne chooseOne;

    public ConsoleHelper(ConsoleOutput out, ConsoleInput in) {
        this.chooseOne = new ChooseOne(out, in);
    }

    public MenuOption chooseOptionUntilValid(String message, MenuOption... options) {
        return chooseOne.chooseWithMessageFrom(message, options)
                .chooseUntilChoiceFound(() -> chooseOne.chooseWithMessageFrom(
                        "\nInput not recognised please try again\n", options));
    }
}
