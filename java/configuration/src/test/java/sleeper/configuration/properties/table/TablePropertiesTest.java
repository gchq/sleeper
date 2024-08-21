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
package sleeper.configuration.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.DummySleeperProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SleeperProperty;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class TablePropertiesTest {

    @Test
    void shouldDefaultToInstancePropertiesValueWhenConfigured() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_PAGE_SIZE, "20");

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);

        // Then
        assertThat(tableProperties.get(PAGE_SIZE)).isEqualTo("20");
    }

    @Test
    void shouldGetDefaultValueFromDefaultProperty() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);

        // Then
        assertThat(tableProperties.get(PAGE_SIZE))
                .isEqualTo("" + (128 * 1024));
        assertThat(PAGE_SIZE.getDefaultValue())
                .isEqualTo("" + (128 * 1024));
    }

    @Test
    void shouldDefaultToAnotherTablePropertyIfConfigured() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "id");

        // When
        TableProperty defaultingProperty = DummyTableProperty.defaultedFrom(TABLE_NAME);

        // Then
        assertThat(tableProperties.get(defaultingProperty)).isEqualTo("id");
    }

    @Test
    void shouldThrowRuntimeExceptionIfConfiguredWithNonInstanceOrNonTableProperty() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "id");

        // When
        SleeperProperty sleeperProperty = new DummySleeperProperty();
        TableProperty defaultingProperty = DummyTableProperty.defaultedFrom(sleeperProperty);

        // Then
        assertThatThrownBy(() -> tableProperties.get(defaultingProperty))
                .hasMessageStartingWith("Unexpected default property type: ");
    }

    @Test
    void shouldReturnTrueForEqualityWhenInstancePropertiesAreEqual() {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        TableProperties duplicateProperties = new TableProperties(instanceProperties);

        // Then
        assertThat(duplicateProperties).isEqualTo(tableProperties);
    }

    @Test
    void shouldReturnFalseForEqualityWhenPropertiesAreDifferent() {
        // Given
        Properties properties = new Properties();
        properties.setProperty("a", "b");
        InstanceProperties instanceProperties = new InstanceProperties(properties);

        Properties differentProperties = new Properties();
        properties.setProperty("a", "c");
        InstanceProperties differentInstanceProperties = new InstanceProperties(differentProperties);

        // When
        TableProperties tableProperties = new TableProperties(instanceProperties);
        TableProperties differentTableProperties = new TableProperties(differentInstanceProperties);

        // Then
        assertThat(differentTableProperties).isNotEqualTo(tableProperties);
    }

    @Test
    void shouldKeepValidationPredicateSameAsOnDefaultProperty() {
        assertThat(TableProperty.getAll().stream()
                .filter(property -> property.getDefaultProperty() != null)
                .filter(property -> property.getDefaultProperty()
                        .validationPredicate() != property.validationPredicate()))
                .isEmpty();
    }

    @Test
    void shouldKeepCDKDeploymentTriggerSameAsOnDefaultProperty() {
        assertThat(TableProperty.getAll().stream()
                .filter(property -> property.getDefaultProperty() != null)
                .filter(property -> property.getDefaultProperty()
                        .isRunCdkDeployWhenChanged() != property.isRunCdkDeployWhenChanged()))
                .isEmpty();
    }

    @Test
    void shouldGetUnknownPropertyValues() {
        // Given
        TableProperties tableProperties = new TableProperties(new InstanceProperties(),
                loadProperties("unknown.property=123"));

        // When / Then
        assertThat(tableProperties.getUnknownProperties())
                .containsExactly(Map.entry("unknown.property", "123"));
    }
}
