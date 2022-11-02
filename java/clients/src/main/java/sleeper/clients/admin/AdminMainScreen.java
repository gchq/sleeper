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
import sleeper.console.ConsoleOutput;

public class AdminMainScreen {

    private final ConsoleOutput out;
    private final ChooseOne chooseOne;

    public AdminMainScreen(ConsoleOutput out, ChooseOne chooseOne) {
        this.out = out;
        this.chooseOne = chooseOne;
    }

    public enum Option implements ChooseOne.Choice {
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

    public Chosen<Option> chooseOption(String message) {
        out.clearScreen(message);
        out.println("ADMINISTRATION COMMAND LINE CLIENT\n----------------------------------\n");
        return chooseOne.chooseFrom(Option.values());
    }

}
