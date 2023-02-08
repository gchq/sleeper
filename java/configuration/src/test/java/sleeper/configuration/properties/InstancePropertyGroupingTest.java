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

import sleeper.configuration.properties.group.PropertyGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstanceProperty.getAllProperties;
import static sleeper.configuration.properties.InstanceProperty.sortProperties;
import static sleeper.configuration.properties.group.InstancePropertyGroup.BULK_IMPORT;
import static sleeper.configuration.properties.group.InstancePropertyGroup.COMMON;
import static sleeper.configuration.properties.group.InstancePropertyGroup.INGEST;

public class InstancePropertyGroupingTest {
    @Test
    void shouldGetAllUserDefinedAndSystemDefinedProperties() {
        // Given/When
        List<InstanceProperty> propertyList = getAllProperties();

        // Then
        assertThat(propertyList)
                .containsAll(UserDefinedInstanceProperty.allList());
        assertThat(propertyList)
                .containsAll(SystemDefinedInstanceProperty.allList());
    }

    @Test
    void shouldOrderPropertiesBasedOnGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty property1 = userProperty("user.property.1", BULK_IMPORT, propertyList::add);
        InstanceProperty property2 = userProperty("user.property.2", INGEST, propertyList::add);
        InstanceProperty property3 = userProperty("user.property.3", COMMON, propertyList::add);

        // When
        List<InstanceProperty> sortedPropertyList = sortProperties(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(property3, property2, property1);
    }

    @Test
    void shouldOrderPropertiesWithDifferentTypesBasedOnGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty userProperty1 = userProperty("user.property.1", BULK_IMPORT, propertyList::add);
        InstanceProperty userProperty2 = userProperty("user.property.2", INGEST, propertyList::add);
        InstanceProperty userProperty3 = userProperty("user.property.3", COMMON, propertyList::add);
        InstanceProperty systemProperty1 = systemProperty("system.property.1", BULK_IMPORT, propertyList::add);
        InstanceProperty systemProperty2 = systemProperty("system.property.2", INGEST, propertyList::add);
        InstanceProperty systemProperty3 = systemProperty("system.property.3", COMMON, propertyList::add);

        // When
        List<InstanceProperty> sortedPropertyList = sortProperties(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(userProperty3, systemProperty3, userProperty2, systemProperty2, userProperty1, systemProperty1);
    }

    @Test
    void shouldOrderPropertiesWithInterweavingTypesBasedOnGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty userProperty1 = userProperty("user.property.1", BULK_IMPORT, propertyList::add);
        InstanceProperty systemProperty1 = systemProperty("system.property.1", BULK_IMPORT, propertyList::add);
        InstanceProperty userProperty2 = userProperty("user.property.2", INGEST, propertyList::add);
        InstanceProperty systemProperty2 = systemProperty("system.property.2", INGEST, propertyList::add);
        InstanceProperty userProperty3 = userProperty("user.property.3", COMMON, propertyList::add);
        InstanceProperty systemProperty3 = systemProperty("system.property.3", COMMON, propertyList::add);

        // When
        List<InstanceProperty> sortedPropertyList = sortProperties(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(userProperty3, systemProperty3, userProperty2, systemProperty2, userProperty1, systemProperty1);
    }

    private static UserDefinedInstanceProperty userProperty(
            String name, PropertyGroup group, Consumer<UserDefinedInstanceProperty> addToList) {
        return UserDefinedInstancePropertyImpl.named(name)
                .propertyGroup(group)
                .addToAllList(addToList).build();
    }

    private static SystemDefinedInstanceProperty systemProperty(
            String name, PropertyGroup group, Consumer<SystemDefinedInstanceProperty> addToList) {
        return SystemDefinedInstancePropertyImpl.named(name)
                .propertyGroup(group)
                .addToAllList(addToList).build();
    }
}
