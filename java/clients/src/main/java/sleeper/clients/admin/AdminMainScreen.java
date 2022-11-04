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
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.UserExitedException;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.MenuOption;

import java.util.Arrays;
import java.util.List;

public class AdminMainScreen {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;

    public AdminMainScreen(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
    }

    public void mainLoop(AdminClient client, String instanceId) {
        List<MenuOption> options = Arrays.asList(
                new MenuOption("Print Sleeper instance property report", () ->
                        client.instancePropertyReport().print(instanceId)),
                new MenuOption("Print Sleeper table names", () ->
                        client.tableNamesReport().print(instanceId)),
                new MenuOption("Print Sleeper table property report", () ->
                        client.tablePropertyReportScreen().chooseTableAndPrint(instanceId)),
                new MenuOption("Update an instance or table property", () ->
                        client.updatePropertyScreen().choosePropertyAndUpdate(instanceId))
        );
        while (true) {
            try {
                chooseOption(options).run();
            } catch (UserExitedException e) {
                break;
            }
        }
    }

    private MenuOption chooseOption(List<MenuOption> options) throws UserExitedException {
        return chooseOption("", options)
                .chooseUntilChoiceFound(() ->
                        chooseOption("\nInput not recognised please try again\n", options));
    }

    private Chosen<MenuOption> chooseOption(String message, List<MenuOption> options) {
        out.clearScreen(message);
        out.println("ADMINISTRATION COMMAND LINE CLIENT\n----------------------------------\n");
        return chooseOne.chooseFrom(options);
    }

}
