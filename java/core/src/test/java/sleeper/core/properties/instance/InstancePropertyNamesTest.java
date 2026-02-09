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

import org.junit.jupiter.api.Test;

import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.table.TableProperty;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
    void shouldNameDefaultPropertiesForTablePropertiesConsistently() {
        assertThat(defaultsOf(TableProperty.getAll()))
                .filteredOn(propertyDefault -> propertyDefault.defaultProperty() instanceof InstanceProperty)
                .allSatisfy(propertyDefault -> assertThat(propertyDefault.defaultPropertyName())
                        .isEqualTo("sleeper.default." + propertyDefault.propertyNameWithoutSleeper()));
    }

    @Test
    void shouldNameDefaultPropertiesForInstancePropertiesConsistently() {
        assertThat(defaultsOf(InstanceProperty.getAll()))
                .extracting(PropertyDefault::defaultPropertyName)
                .allSatisfy(name -> assertThat(name)
                        .doesNotStartWith("sleeper.default.table."));
    }

    private static Stream<PropertyDefault> defaultsOf(List<? extends SleeperProperty> properties) {
        return properties.stream()
                .flatMap(property -> Optional.ofNullable(property.getDefaultProperty()).stream()
                        .map(defaultProperty -> new PropertyDefault(property, defaultProperty)));
    }

    public record PropertyDefault(SleeperProperty property, SleeperProperty defaultProperty) {

        public String defaultPropertyName() {
            return defaultProperty.getPropertyName();
        }

        public String propertyNameWithoutSleeper() {
            return property.getPropertyName().substring("sleeper.".length());
        }
    }
}
