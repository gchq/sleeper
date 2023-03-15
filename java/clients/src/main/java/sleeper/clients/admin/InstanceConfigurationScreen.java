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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.MenuOption;

import java.io.IOException;
import java.io.UncheckedIOException;

public class InstanceConfigurationScreen {
    private final ConsoleOutput out;
    private final ChooseOne chooseOne;
    private final AdminConfigStore store;
    private final UpdatePropertiesWithNano editor;

    public InstanceConfigurationScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store, UpdatePropertiesWithNano editor) {
        this.out = out;
        this.chooseOne = new ChooseOne(out, in);
        this.store = store;
        this.editor = editor;
    }

    public void viewAndEditProperties(String instanceId) throws InterruptedException {
        viewAndEditProperties(store.loadInstanceProperties(instanceId), PropertiesDiff.noChanges());
    }

    public void viewAndEditProperties(InstanceProperties properties, PropertiesDiff changesSoFar) throws InterruptedException {
        UpdatePropertiesRequest<InstanceProperties> request = openFile(properties);
        PropertiesDiff changes = changesSoFar.andThen(request.getDiff());
        if (changes.isChanged()) {
            changes.print(out, properties.getPropertiesIndex());
            chooseOne.chooseFrom(
                    new MenuOption("Apply changes", () -> {
                    }),
                    new MenuOption("Return to editor", () -> viewAndEditProperties(request.getUpdatedProperties(), changes)),
                    new MenuOption("Discard changes and return to main menu", () -> {
                    })).getChoice().orElse(MenuOption.none()).run();
        }
    }

    private UpdatePropertiesRequest<InstanceProperties> openFile(InstanceProperties properties) throws InterruptedException {
        try {
            return editor.openPropertiesFile(properties);
        } catch (IOException e1) {
            throw new UncheckedIOException(e1);
        }
    }
}
