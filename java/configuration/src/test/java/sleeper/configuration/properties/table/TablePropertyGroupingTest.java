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

package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.PropertyGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.sortProperties;
import static sleeper.configuration.properties.table.TablePropertyGroup.CONFIGURATION;

public class TablePropertyGroupingTest {
    @Test
    void shouldPreserveOrderingWithinAGroup() {
        // Given
        List<TableProperty> properties = new ArrayList<>();
        TableProperty property1 = tableProperty("table.property.1", CONFIGURATION, properties::add);
        TableProperty property2 = tableProperty("table.property.second", CONFIGURATION, properties::add);
        TableProperty property3 = tableProperty("table.property.c", CONFIGURATION, properties::add);

        // When
        List<TableProperty> sortedProperties = sortProperties(properties);

        // Then
        assertThat(sortedProperties)
                .containsExactly(property1, property2, property3);
    }

    private static TableProperty tableProperty(String name, PropertyGroup group, Consumer<TableProperty> addToList) {
        return TablePropertyImpl.named(name).propertyGroup(group).addToList(addToList).build();
    }
}
