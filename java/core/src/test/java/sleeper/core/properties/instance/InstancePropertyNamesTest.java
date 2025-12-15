/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.properties.instance;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.table.TableProperty;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class InstancePropertyNamesTest {

    @Test
    void shouldStartInstancePropertyNamesWithSleeper() {
        assertThat(InstanceProperty.getAll())
                .extracting(InstanceProperty::getPropertyName)
                .allSatisfy(name -> assertThat(name).startsWith("sleeper."));
    }

    @Test
    void shouldNotNameInstancePropertiesAsTableProperties() {
        assertThat(InstanceProperty.getAll())
                .extracting(InstanceProperty::getPropertyName)
                .allSatisfy(name -> assertThat(name).doesNotStartWith("sleeper.table."));
    }

    @Test
    @Disabled("TODO")
    void shouldNameDefaultPropertiesForTablePropertiesConsistently() {
        assertThat(defaultPropertiesOfTableProperties())
                .extracting(InstanceProperty::getPropertyName)
                .allSatisfy(name -> assertThat(name).startsWith("sleeper.default.table."));
    }

    @Test
    void shouldNameDefaultPropertiesForInstancePropertiesConsistently() {
        // TODO
    }

    @Test
    void shouldNotNameNonDefaultPropertiesAsDefaultProperties() {
        // TODO
    }

    private static List<InstanceProperty> defaultPropertiesOfTableProperties() {
        return TableProperty.getAll().stream()
                .flatMap(property -> Optional.ofNullable(property.getDefaultProperty()).stream())
                .filter(InstanceProperty.class::isInstance)
                .map(property -> (InstanceProperty) property)
                .toList();
    }
}
