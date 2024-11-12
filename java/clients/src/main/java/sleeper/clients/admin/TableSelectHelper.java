/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.UserExitedException;
import sleeper.clients.util.console.menu.ChooseOne;
import sleeper.clients.util.console.menu.Chosen;
import sleeper.clients.util.console.menu.ConsoleChoice;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.RETURN_TO_MAIN_MENU;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadInstanceProperties;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadTableProperties;

public class TableSelectHelper {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;
    private final AdminClientPropertiesStore store;

    public TableSelectHelper(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
        this.store = store;
    }

    public Optional<TableProperties> chooseTableOrReturnToMain(String instanceId) throws UserExitedException {
        Optional<InstanceProperties> properties = tryLoadInstanceProperties(out, in, store, instanceId);
        if (properties.isPresent()) {
            return chooseTableOrReturnToMain(properties.get());
        }
        return Optional.empty();
    }

    public Optional<TableProperties> chooseTableOrReturnToMain(InstanceProperties properties) throws UserExitedException {
        Chosen<ConsoleChoice> chosen = chooseTable("")
                .chooseUntilSomethingEntered(() -> chooseTable("\nYou did not enter anything please try again\n"));
        if (chosen.getChoice().isPresent()) {
            // Return to main screen
            return Optional.empty();
        }
        return tryLoadTableProperties(out, in, store, properties, chosen.getEntered());
    }

    private Chosen<ConsoleChoice> chooseTable(String message) {
        out.clearScreen(message);
        out.println("Which TABLE do you want to select?\n");
        return chooseOne.chooseWithMessageFrom(
                "Please enter the TABLE NAME now or use the following options:",
                RETURN_TO_MAIN_MENU);
    }
}
