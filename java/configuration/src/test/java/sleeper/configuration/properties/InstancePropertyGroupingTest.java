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

package sleeper.configuration.properties;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstanceProperty.getAll;
import static sleeper.configuration.properties.InstancePropertyGroup.BULK_IMPORT;
import static sleeper.configuration.properties.InstancePropertyGroup.COMMON;
import static sleeper.configuration.properties.InstancePropertyGroup.INGEST;
import static sleeper.configuration.properties.InstancePropertyGroup.sortPropertiesByGroup;

class InstancePropertyGroupingTest {
    @Test
    void shouldGetAllUserDefinedAndSystemDefinedProperties() {
        // Given/When
        List<InstanceProperty> propertyList = getAll();

        // Then
        assertThat(propertyList)
                .containsAll(UserDefinedInstanceProperty.getAll())
                .containsAll(SystemDefinedInstanceProperty.getAll());
    }

    @Test
    void shouldOrderByGroup() {
        // Given groups are out of order and each property has a different group
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty property1 = userProperty("user.property.1", BULK_IMPORT, propertyList::add);
        InstanceProperty property2 = userProperty("user.property.2", INGEST, propertyList::add);
        InstanceProperty property3 = userProperty("user.property.3", COMMON, propertyList::add);

        // When
        List<InstanceProperty> sortedPropertyList = sortPropertiesByGroup(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(property3, property2, property1);
    }

    @Test
    void shouldPreserveOrderingWithinAGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty property1 = userProperty("user.property.1", COMMON, propertyList::add);
        InstanceProperty property2 = userProperty("user.property.second", COMMON, propertyList::add);
        InstanceProperty property3 = userProperty("user.property.c", COMMON, propertyList::add);

        // When
        List<InstanceProperty> sortedPropertyList = sortPropertiesByGroup(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(property1, property2, property3);
    }

    @Test
    void shouldBringPropertiesInAGroupTogetherWhenNotSpecifiedTogether() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty userProperty1 = userProperty("user.property.1", COMMON, propertyList::add);
        InstanceProperty userProperty2 = userProperty("user.property.2", INGEST, propertyList::add);
        InstanceProperty userProperty3 = userProperty("user.property.3", BULK_IMPORT, propertyList::add);
        InstanceProperty systemProperty1 = systemProperty("system.property.1", COMMON, propertyList::add);
        InstanceProperty systemProperty2 = systemProperty("system.property.2", INGEST, propertyList::add);
        InstanceProperty systemProperty3 = systemProperty("system.property.3", BULK_IMPORT, propertyList::add);

        // When
        List<InstanceProperty> sortedPropertyList = sortPropertiesByGroup(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(userProperty1, systemProperty1, userProperty2, systemProperty2, userProperty3, systemProperty3);
    }

    private static UserDefinedInstanceProperty userProperty(
            String name, PropertyGroup group, Consumer<UserDefinedInstanceProperty> addToList) {
        return UserDefinedInstancePropertyImpl.named(name)
                .propertyGroup(group)
                .description("Test user property")
                .addToAllList(addToList).build();
    }

    private static SystemDefinedInstanceProperty systemProperty(
            String name, PropertyGroup group, Consumer<SystemDefinedInstanceProperty> addToList) {
        return SystemDefinedInstancePropertyImpl.named(name)
                .propertyGroup(group)
                .description("Test system property")
                .addToAllList(addToList).build();
    }
}
