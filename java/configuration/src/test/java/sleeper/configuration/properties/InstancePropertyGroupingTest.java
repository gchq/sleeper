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
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InstancePropertyGroupingTest {
    @Test
    void shouldGetAllUserDefinedAndSystemDefinedProperties() {
        // Given/When
        List<InstanceProperty> propertyList = InstanceProperty.getAllProperties();

        // Then
        assertThat(propertyList)
                .containsAll(Arrays.asList(UserDefinedInstanceProperty.values()));
        assertThat(propertyList)
                .containsAll(Arrays.asList(SystemDefinedInstanceProperty.values()));
    }

    @Test
    void shouldGetAllUserDefinedPropertiesOrderedByGroup() {
        // Given/When
        List<InstanceProperty> propertyList = InstanceProperty.getAllGroupedProperties();
        List<InstanceProperty> sortedUserDefinedProperties = InstanceProperty.sortProperties(
                Arrays.asList(UserDefinedInstanceProperty.values()));

        // Then
        assertThat(propertyList)
                .containsSubsequence(sortedUserDefinedProperties);
    }

    @Test
    void shouldGetAllSystemDefinedPropertiesOrderedByGroup() {
        // Given/When
        List<InstanceProperty> propertyList = InstanceProperty.getAllGroupedProperties();
        List<InstanceProperty> sortedSystemDefinedProperties = InstanceProperty.sortProperties(
                Arrays.asList(SystemDefinedInstanceProperty.values()));

        // Then
        assertThat(propertyList)
                .containsSubsequence(sortedSystemDefinedProperties);
    }

    @Test
    void shouldOrderPropertiesBasedOnGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty property1 = UserDefinedInstancePropertyImpl.named("Test Property 1")
                .propertyGroup(PropertyGroup.BULK_IMPORT)
                .addToAllList(propertyList::add).build();
        InstanceProperty property2 = UserDefinedInstancePropertyImpl.named("Test Property 2")
                .propertyGroup(PropertyGroup.INGEST)
                .addToAllList(propertyList::add).build();
        InstanceProperty property3 = UserDefinedInstancePropertyImpl.named("Test Property 3")
                .propertyGroup(PropertyGroup.COMMON)
                .addToAllList(propertyList::add).build();

        // When
        List<InstanceProperty> sortedPropertyList = InstanceProperty.sortProperties(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(property3, property2, property1);
    }

    @Test
    void shouldOrderPropertiesWithDifferentTypesBasedOnGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty userProperty1 = UserDefinedInstancePropertyImpl.named("User Property 1")
                .propertyGroup(PropertyGroup.BULK_IMPORT)
                .addToAllList(propertyList::add).build();
        InstanceProperty userProperty2 = UserDefinedInstancePropertyImpl.named("User Property 2")
                .propertyGroup(PropertyGroup.INGEST)
                .addToAllList(propertyList::add).build();
        InstanceProperty userProperty3 = UserDefinedInstancePropertyImpl.named("User Property 3")
                .propertyGroup(PropertyGroup.COMMON)
                .addToAllList(propertyList::add).build();
        InstanceProperty systemProperty1 = UserDefinedInstancePropertyImpl.named("System Property 1")
                .propertyGroup(PropertyGroup.BULK_IMPORT)
                .addToAllList(propertyList::add).build();
        InstanceProperty systemProperty2 = UserDefinedInstancePropertyImpl.named("System Property 2")
                .propertyGroup(PropertyGroup.INGEST)
                .addToAllList(propertyList::add).build();
        InstanceProperty systemProperty3 = UserDefinedInstancePropertyImpl.named("System Property 3")
                .propertyGroup(PropertyGroup.COMMON)
                .addToAllList(propertyList::add).build();

        // When
        List<InstanceProperty> sortedPropertyList = InstanceProperty.sortProperties(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(userProperty3, systemProperty3, userProperty2, systemProperty2, userProperty1, systemProperty1);
    }

    @Test
    void shouldOrderPropertiesWithInterweavingTypesBasedOnGroup() {
        // Given
        List<InstanceProperty> propertyList = new ArrayList<>();
        InstanceProperty userProperty1 = UserDefinedInstancePropertyImpl.named("User Property 1")
                .propertyGroup(PropertyGroup.BULK_IMPORT)
                .addToAllList(propertyList::add).build();
        InstanceProperty systemProperty1 = UserDefinedInstancePropertyImpl.named("System Property 1")
                .propertyGroup(PropertyGroup.BULK_IMPORT)
                .addToAllList(propertyList::add).build();
        InstanceProperty userProperty2 = UserDefinedInstancePropertyImpl.named("User Property 2")
                .propertyGroup(PropertyGroup.INGEST)
                .addToAllList(propertyList::add).build();
        InstanceProperty systemProperty2 = UserDefinedInstancePropertyImpl.named("System Property 2")
                .propertyGroup(PropertyGroup.INGEST)
                .addToAllList(propertyList::add).build();
        InstanceProperty userProperty3 = UserDefinedInstancePropertyImpl.named("User Property 3")
                .propertyGroup(PropertyGroup.COMMON)
                .addToAllList(propertyList::add).build();
        InstanceProperty systemProperty3 = UserDefinedInstancePropertyImpl.named("System Property 3")
                .propertyGroup(PropertyGroup.COMMON)
                .addToAllList(propertyList::add).build();

        // When
        List<InstanceProperty> sortedPropertyList = InstanceProperty.sortProperties(propertyList);

        // Then
        assertThat(sortedPropertyList)
                .containsExactly(userProperty3, systemProperty3, userProperty2, systemProperty2, userProperty1, systemProperty1);
    }
}
