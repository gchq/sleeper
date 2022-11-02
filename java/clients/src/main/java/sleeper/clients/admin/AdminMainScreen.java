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
package sleeper.clients.admin;

import sleeper.clients.AdminClient;
import sleeper.console.ChooseOne;
import sleeper.console.Chosen;
import sleeper.console.ConsoleChoice;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

import java.util.Optional;

public class AdminMainScreen {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;

    public AdminMainScreen(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
    }

    public enum Option implements ConsoleChoice {
        PRINT_PROPERTY_REPORT("Print Sleeper instance property report"),
        PRINT_TABLE_NAMES("Print Sleeper table names"),
        PRINT_TABLE_PROPERTY_REPORT("Print Sleeper table property report"),
        UPDATE_A_PROPERTY("Update an instance or table property");

        private final String description;

        Option(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public void mainLoop(AdminClient client, String instanceId) {
        Chosen<Option> chosen = chooseOption("");
        while (!chosen.isExit()) {
            Optional<Option> choice = chosen.getChoice();
            if (choice.isPresent()) {
                switch (choice.get()) {
                    case PRINT_PROPERTY_REPORT:
                        client.instancePropertyReport().print(instanceId);
                        confirmReturnToMainScreen();
                        break;
                    case PRINT_TABLE_PROPERTY_REPORT:
                        if (client.tablePropertyReportScreen().chooseTableAndPrint(instanceId)
                                .isExit()) {
                            return;
                        }
                        confirmReturnToMainScreen();
                        break;
                    case PRINT_TABLE_NAMES:
                    case UPDATE_A_PROPERTY:
                        break;
                }
            }
            chosen = chooseOption("");
        }
    }

    private Chosen<Option> chooseOption(String message) {
        out.clearScreen(message);
        out.println("ADMINISTRATION COMMAND LINE CLIENT\n----------------------------------\n");
        return chooseOne.chooseFrom(Option.values());
    }

    private void confirmReturnToMainScreen() {
        out.println("\n\n----------------------------------");
        out.println("Hit enter to return to main screen");
        in.waitForLine();
    }

}
