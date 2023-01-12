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

import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.UserExitedException;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.ConsoleChoice;

import static sleeper.clients.admin.AdminCommonPrompts.INPUT_EMPTY;
import static sleeper.clients.admin.AdminCommonPrompts.RETURN_TO_MAIN_MENU;
import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class UpdatePropertyScreen {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;
    private final AdminConfigStore store;

    public UpdatePropertyScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
        this.store = store;
    }

    public void choosePropertyAndUpdate(String instanceId) throws UserExitedException {
        while (true) {
            try {
                chooseProperty().ifEnteredNonChoiceValue(propertyName ->
                        choosePropertyValue().ifEnteredNonChoiceValue(propertyValue ->
                                updatePropertyIfValid(instanceId, propertyName, propertyValue)));
                break;
            } catch (IllegalArgumentException e) {
                out.println(e.getMessage());
                confirmReturnToEnterProperty();
            }
        }
    }

    private void updatePropertyIfValid(String instanceId, String propertyName, String propertyValue) {
        if (propertyName.startsWith("sleeper.table")) {
            chooseTable().ifEnteredNonChoiceValue(tableName -> {
                TableProperty property = SleeperPropertyUtils.getValidTableProperty(propertyName, propertyValue);
                store.updateTableProperty(instanceId, tableName, property, propertyValue);
                out.println(propertyName + " has been updated to " + propertyValue);
                confirmReturnToMainScreen(out, in);
            });
        } else {
            UserDefinedInstanceProperty property = SleeperPropertyUtils.getValidInstanceProperty(propertyName, propertyValue);
            store.updateInstanceProperty(instanceId, property, propertyValue);
            out.println(propertyName + " has been updated to " + propertyValue);
            confirmReturnToMainScreen(out, in);
        }
    }

    private Chosen<ConsoleChoice> chooseProperty() throws UserExitedException {
        return chooseProperty("")
                .chooseUntilSomethingEntered(() ->
                        chooseProperty(INPUT_EMPTY));
    }

    private Chosen<ConsoleChoice> chooseProperty(String message) {
        out.clearScreen(message);
        out.println("What is the PROPERTY NAME of the property that you would like to update?\n");
        return chooseOne.chooseWithMessageFrom(
                "Please enter the PROPERTY NAME now or use the following options:",
                RETURN_TO_MAIN_MENU);
    }

    private Chosen<ConsoleChoice> choosePropertyValue() throws UserExitedException {
        return choosePropertyValue("")
                .chooseUntilSomethingEntered(() ->
                        choosePropertyValue(INPUT_EMPTY));
    }

    private Chosen<ConsoleChoice> choosePropertyValue(String message) {
        out.clearScreen(message);
        out.println("What is the new PROPERTY VALUE?\n");
        return chooseOne.chooseWithMessageFrom(
                "Please enter the PROPERTY VALUE now or use the following options:",
                RETURN_TO_MAIN_MENU);
    }

    private Chosen<ConsoleChoice> chooseTable() throws UserExitedException {
        return chooseTable("")
                .chooseUntilSomethingEntered(() ->
                        chooseTable(INPUT_EMPTY));
    }

    private Chosen<ConsoleChoice> chooseTable(String message) {
        out.clearScreen(message);
        out.println("As the property name begins with sleeper.table we also need to know the TABLE you want to update\n");
        return chooseOne.chooseWithMessageFrom(
                "Please enter the TABLE NAME now or use the following options:",
                RETURN_TO_MAIN_MENU);
    }

    private void confirmReturnToEnterProperty() {
        out.println("\n\n----------------------------------");
        out.println("Hit enter to return to the property screen so you can adjust the property and continue");
        in.waitForLine();
    }
}
