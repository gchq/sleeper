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
import sleeper.console.menu.ConsoleChoice;

public class AdminCommonPrompts {
    private AdminCommonPrompts() {
    }

    public static final ConsoleChoice RETURN_TO_MAIN_MENU = ConsoleChoice.describedAs("Return to Main Menu");

    public static void confirmReturnToMainScreen(ConsoleOutput out, ConsoleInput in) {
        out.println("\n\n----------------------------------");
        out.println("Hit enter to return to main screen");
        in.waitForLine();
    }
}
