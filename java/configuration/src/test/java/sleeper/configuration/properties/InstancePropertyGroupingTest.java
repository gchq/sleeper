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
                .containsSequence(List.of(property3, property2, property1));
    }
}
