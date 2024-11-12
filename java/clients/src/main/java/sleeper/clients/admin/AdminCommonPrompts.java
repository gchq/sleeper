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
import sleeper.clients.util.console.menu.ConsoleChoice;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Optional;
import java.util.function.Supplier;

public class AdminCommonPrompts {
    private AdminCommonPrompts() {
    }

    public static final ConsoleChoice RETURN_TO_MAIN_MENU = ConsoleChoice.describedAs("Return to Main Menu");

    public static void confirmReturnToMainScreen(ConsoleOutput out, ConsoleInput in) {
        out.println("\n\n----------------------------------");
        out.println("Hit enter to return to main screen");
        in.waitForLine();
    }

    public static Optional<InstanceProperties> tryLoadInstanceProperties(
            ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store, String instanceId) {
        return tryLoadInstanceProperties(out, in, () -> store.loadInstanceProperties(instanceId));
    }

    public static Optional<InstanceProperties> tryLoadInstanceProperties(
            ConsoleOutput out, ConsoleInput in, Supplier<InstanceProperties> loadInstanceProperties) {
        try {
            return Optional.of(loadInstanceProperties.get());
        } catch (AdminClientPropertiesStore.CouldNotLoadInstanceProperties e) {
            out.println();
            e.print(out);
            confirmReturnToMainScreen(out, in);
            return Optional.empty();
        }
    }

    public static Optional<TableProperties> tryLoadTableProperties(
            ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store, InstanceProperties properties, String tableName) {
        return tryLoadTableProperties(out, in, () -> store.loadTableProperties(properties, tableName));
    }

    public static Optional<TableProperties> tryLoadTableProperties(
            ConsoleOutput out, ConsoleInput in, Supplier<TableProperties> loadTableProperties) {
        try {
            return Optional.of(loadTableProperties.get());
        } catch (AdminClientPropertiesStore.CouldNotLoadTableProperties e) {
            out.println();
            e.print(out);
        }
        confirmReturnToMainScreen(out, in);
        return Optional.empty();
    }
}
