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
import sleeper.configuration.properties.InstancePropertyGroup;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertyGroup;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.ConsoleChoice;
import sleeper.console.menu.MenuOption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static sleeper.clients.admin.AdminCommonPrompts.RETURN_TO_MAIN_MENU;

public class InstanceConfigurationScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;
    private final TableSelectHelper selectTable;
    private final AdminConfigStore store;
    private final UpdatePropertiesWithNano editor;

    public InstanceConfigurationScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store, UpdatePropertiesWithNano editor) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
        this.selectTable = new TableSelectHelper(out, in, store);
        this.store = store;
        this.editor = editor;
    }

    public void viewAndEditProperties(String instanceId) throws InterruptedException {
        withInstanceProperties(store.loadInstanceProperties(instanceId))
                .viewAndEditProperties();
    }

    public void viewAndEditTableProperties(String instanceId) throws InterruptedException {
        Optional<TableProperties> tableOpt = selectTable.chooseTableOrReturnToMain(instanceId);
        if (tableOpt.isPresent()) {
            withTableProperties(instanceId, tableOpt.get())
                    .viewAndEditProperties();
        }
    }

    public void choosePropertyGroup(String instanceId) throws InterruptedException {
        out.clearScreen("");
        Map<ConsoleChoice, PropertyGroup> choiceToInstanceGroup = new LinkedHashMap<>();
        Map<ConsoleChoice, PropertyGroup> choiceToTableGroup = new LinkedHashMap<>();
        InstancePropertyGroup.getAll().forEach(group ->
                choiceToInstanceGroup.put(ConsoleChoice.describedAs("Instance Properties - " + group.getName()), group));
        TablePropertyGroup.getAll().forEach(group ->
                choiceToTableGroup.put(ConsoleChoice.describedAs("Table Properties - " + group.getName()), group));
        List<ConsoleChoice> choices = new ArrayList<>();
        choices.add(RETURN_TO_MAIN_MENU);
        choices.addAll(choiceToInstanceGroup.keySet());
        choices.addAll(choiceToTableGroup.keySet());
        Chosen<ConsoleChoice> chosen = chooseOne.chooseWithMessageFrom(
                "Please select a group from the below options and hit return:",
                choices);
        Optional<PropertyGroup> instanceGroupOpt = chosen.getChoice().map(choiceToInstanceGroup::get);
        if (instanceGroupOpt.isPresent()) {
            withGroupedInstanceProperties(store.loadInstanceProperties(instanceId), instanceGroupOpt.get())
                    .viewAndEditProperties();
        }
        Optional<PropertyGroup> tableGroupOpt = chosen.getChoice().map(choiceToTableGroup::get);
        if (tableGroupOpt.isPresent()) {
            Optional<TableProperties> tableOpt = selectTable.chooseTableOrReturnToMain(instanceId);
            if (tableOpt.isPresent()) {
                withGroupedTableProperties(instanceId, tableOpt.get(), tableGroupOpt.get())
                        .viewAndEditProperties();
            }
        }
    }

    private WithProperties<InstanceProperties> withInstanceProperties(InstanceProperties properties) {
        return new WithProperties<>(properties, editor::openPropertiesFile, store::saveInstanceProperties);
    }

    private WithProperties<InstanceProperties> withGroupedInstanceProperties(InstanceProperties properties, PropertyGroup group) {
        return new WithProperties<>(properties, props -> editor.openPropertiesFile(props, group), store::saveInstanceProperties);
    }

    private WithProperties<TableProperties> withTableProperties(String instanceId, TableProperties properties) {
        return new WithProperties<>(properties, editor::openPropertiesFile,
                (tableProperties, diff) -> store.saveTableProperties(instanceId, tableProperties, diff));
    }

    private WithProperties<TableProperties> withGroupedTableProperties(
            String instanceId, TableProperties properties, PropertyGroup group) {
        return new WithProperties<>(properties, props -> editor.openPropertiesFile(props, group),
                (tableProperties, diff) -> store.saveTableProperties(instanceId, tableProperties, diff));
    }

    private interface OpenFile<T extends SleeperProperties<?>> {
        UpdatePropertiesRequest<T> openFile(T properties) throws IOException, InterruptedException;
    }

    private interface SaveChanges<T extends SleeperProperties<?>> {
        void saveChanges(T properties, PropertiesDiff diff);
    }

    private class WithProperties<T extends SleeperProperties<?>> {

        private final T properties;
        private final OpenFile<T> editor;
        private final SaveChanges<T> store;

        WithProperties(T properties, OpenFile<T> editor, SaveChanges<T> store) {
            this.properties = properties;
            this.editor = editor;
            this.store = store;
        }

        WithProperties<T> withProperties(T properties) {
            return new WithProperties<>(properties, editor, store);
        }

        void viewAndEditProperties() throws InterruptedException {
            viewAndEditProperties(PropertiesDiff.noChanges());
        }

        void viewAndEditProperties(PropertiesDiff changesSoFar) throws InterruptedException {
            UpdatePropertiesRequest<T> request = openPropertiesFile();
            PropertiesDiff changes = changesSoFar.andThen(request.getDiff());
            if (changes.isChanged()) {
                Set<SleeperProperty> invalidProperties = request.getInvalidProperties();
                changes.print(out, properties.getPropertiesIndex(), invalidProperties);

                chooseFromOptions(request.getUpdatedProperties(), changes, invalidProperties.isEmpty());
            }
        }

        void chooseFromOptions(
                T updatedProperties, PropertiesDiff changes, boolean valid) throws InterruptedException {
            MenuOption saveChanges = new MenuOption("Save changes", () -> {
                try {
                    store.saveChanges(updatedProperties, changes);
                    out.println("\n\n----------------------------------");
                    out.println("Saved successfully, hit enter to return to main screen");
                    in.waitForLine();
                } catch (AdminConfigStore.CouldNotSaveProperties e) {
                    out.println("\n\n----------------------------------\n");
                    e.print(out);
                    out.println();
                    chooseFromOptions(updatedProperties, changes, valid);
                }
            });
            MenuOption returnToEditor = new MenuOption("Return to editor", () ->
                    withProperties(updatedProperties).viewAndEditProperties(changes));
            MenuOption discardChanges = new MenuOption("Discard changes and return to main menu", () -> {
            });
            if (valid) {
                chooseOne.chooseFrom(saveChanges, returnToEditor, discardChanges)
                        .getChoice().orElse(returnToEditor).run();
            } else {
                chooseOne.chooseFrom(returnToEditor, discardChanges)
                        .getChoice().orElse(returnToEditor).run();
            }
        }

        UpdatePropertiesRequest<T> openPropertiesFile() throws InterruptedException {
            try {
                return editor.openFile(properties);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
