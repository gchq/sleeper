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
import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.clients.admin.properties.PropertyGroupSelectHelper;
import sleeper.clients.admin.properties.PropertyGroupWithCategory;
import sleeper.clients.admin.properties.UpdatePropertiesRequest;
import sleeper.clients.admin.properties.UpdatePropertiesWithTextEditor;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.menu.ChooseOne;
import sleeper.clients.util.console.menu.MenuOption;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Set;

public class InstanceConfigurationScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;
    private final PropertyGroupSelectHelper selectGroup;
    private final TableSelectHelper selectTable;
    private final AdminClientPropertiesStore store;
    private final UpdatePropertiesWithTextEditor editor;

    public InstanceConfigurationScreen(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store, UpdatePropertiesWithTextEditor editor) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
        this.selectGroup = new PropertyGroupSelectHelper(out, in);
        this.selectTable = new TableSelectHelper(out, in, store);
        this.store = store;
        this.editor = editor;
    }

    public void viewAndEditProperties(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> properties = tryLoadInstanceProperties(instanceId);
        if (properties.isPresent()) {
            withInstanceProperties(properties.get())
                    .viewAndEditProperties();
        }
    }

    public void viewAndEditTableProperties(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> properties = tryLoadInstanceProperties(instanceId);
        if (properties.isPresent()) {
            Optional<TableProperties> tableOpt = selectTable.chooseTableOrReturnToMain(properties.get());
            if (tableOpt.isPresent()) {
                withTableProperties(properties.get(), tableOpt.get())
                        .viewAndEditProperties();
            }
        }
    }

    public void viewAndEditPropertyGroup(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> properties = tryLoadInstanceProperties(instanceId);
        if (properties.isPresent()) {
            Optional<WithProperties<?>> withProperties = selectGroup.selectPropertyGroup()
                    .flatMap(group -> withPropertyGroup(properties.get(), group));
            if (withProperties.isPresent()) {
                withProperties.get().viewAndEditProperties();
            }
        }
    }

    private Optional<WithProperties<?>> withPropertyGroup(InstanceProperties properties, PropertyGroupWithCategory group) {
        if (group.isInstancePropertyGroup()) {
            return Optional.of(withGroupedInstanceProperties(properties, group.getGroup()));
        } else if (group.isTablePropertyGroup()) {
            return selectTable.chooseTableOrReturnToMain(properties)
                    .map(table -> withGroupedTableProperties(properties, table, group.getGroup()));
        }
        return Optional.empty();
    }

    private WithProperties<InstanceProperties> withInstanceProperties(InstanceProperties properties) {
        return new WithProperties<>(properties, editor::openPropertiesFile, store::saveInstanceProperties);
    }

    private WithProperties<InstanceProperties> withGroupedInstanceProperties(InstanceProperties properties, PropertyGroup group) {
        return new WithProperties<>(properties, props -> editor.openPropertiesFile(props, group), store::saveInstanceProperties);
    }

    private WithProperties<TableProperties> withTableProperties(
            InstanceProperties instanceProperties, TableProperties properties) {
        return new WithProperties<>(properties, editor::openPropertiesFile,
                (tableProperties, diff) -> store.saveTableProperties(instanceProperties, tableProperties));
    }

    private WithProperties<TableProperties> withGroupedTableProperties(
            InstanceProperties instanceProperties, TableProperties properties, PropertyGroup group) {
        return new WithProperties<>(properties, props -> editor.openPropertiesFile(props, group),
                (tableProperties, diff) -> store.saveTableProperties(instanceProperties, tableProperties));
    }

    private Optional<InstanceProperties> tryLoadInstanceProperties(String instanceId) {
        return AdminCommonPrompts.tryLoadInstanceProperties(out, in, store, instanceId);
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
                } catch (AdminClientPropertiesStore.CouldNotSaveProperties e) {
                    out.println("\n\n----------------------------------\n");
                    e.print(out);
                    out.println();
                    chooseFromOptions(updatedProperties, changes, valid);
                }
            });
            MenuOption returnToEditor = new MenuOption("Return to editor", () -> withProperties(updatedProperties).viewAndEditProperties(changes));
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
