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
package sleeper.clients.admin.properties;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.menu.ChooseOne;
import sleeper.clients.util.console.menu.ConsoleChoice;
import sleeper.configuration.properties.instance.InstancePropertyGroup;
import sleeper.configuration.properties.table.TablePropertyGroup;
import sleeper.core.properties.PropertyGroup;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.RETURN_TO_MAIN_MENU;

public class PropertyGroupSelectHelper {
    private final ConsoleOutput out;
    private final ChooseOne chooseOne;

    public PropertyGroupSelectHelper(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.chooseOne = new ChooseOne(out, in);
    }

    public Optional<PropertyGroupWithCategory> selectPropertyGroup() {
        out.clearScreen("");

        Map<ConsoleChoice, PropertyGroupWithCategory> choiceToGroup = new LinkedHashMap<>();
        addToChoiceMap(PropertyGroupWithCategory.INSTANCE, InstancePropertyGroup.getAll(), choiceToGroup);
        addToChoiceMap(PropertyGroupWithCategory.TABLE, TablePropertyGroup.getAll(), choiceToGroup);

        List<ConsoleChoice> choices = new ArrayList<>();
        choices.add(RETURN_TO_MAIN_MENU);
        choices.addAll(choiceToGroup.keySet());

        return chooseOne.chooseWithMessageFrom(
                "Please select a group from the below options and hit return:",
                choices)
                .getChoice().map(choiceToGroup::get);
    }

    private static void addToChoiceMap(
            PropertyGroupWithCategory.Category category, List<PropertyGroup> groups,
            Map<ConsoleChoice, PropertyGroupWithCategory> choices) {
        groups.forEach(group -> choices.put(ConsoleChoice.describedAs(category.getName() + " - " + group.getName()),
                new PropertyGroupWithCategory(group, category)));
    }
}
