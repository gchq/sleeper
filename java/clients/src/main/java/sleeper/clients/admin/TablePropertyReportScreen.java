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

import sleeper.console.ChooseOne;
import sleeper.console.Chosen;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

public class TablePropertyReportScreen {

    private final ConsoleOutput out;
    private final ChooseOne chooseOne;
    private final AdminConfigStore store;

    public TablePropertyReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.chooseOne = new ChooseOne(out, in);
        this.store = store;
    }

    public enum Option implements ChooseOne.Choice {
        RETURN_TO_MAIN_MENU("Return to Main Menu");

        private final String description;

        Option(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public Chosen<TablePropertyReportScreen.Option> chooseTableAndPrint(String instanceId) {
        Chosen<TablePropertyReportScreen.Option> chosen = chooseTable();
        if (!chosen.isExit() && !chosen.getChoice().isPresent()) {
            new TablePropertyReport(out, store).print(instanceId, chosen.getEntered());
        }
        return chosen;
    }

    public Chosen<TablePropertyReportScreen.Option> chooseTable() {
        return chooseTable("");
    }

    private Chosen<TablePropertyReportScreen.Option> chooseTable(String message) {
        out.clearScreen(message);
        out.println("Which TABLE do you want to check?\n");
        Chosen<TablePropertyReportScreen.Option> chosen = chooseOne.chooseWithMessageFrom(
                "Please enter the TABLE NAME now or use the following options:",
                TablePropertyReportScreen.Option.values());
        if ("".equals(chosen.getEntered())) {
            return chooseTable("\nYou did not enter anything please try again\n");
        }
        return chosen;
    }
}
