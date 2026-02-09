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
package sleeper.core.properties.table;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.testutils.DummySleeperProperty;
import sleeper.core.properties.testutils.DummyTableProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

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
        TableProperty defaultingProperty = DummyTableProperty.defaultedFrom(TABLE_NAME);
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(TABLE_NAME, "some-table");

        // When
        String value = tableProperties.get(defaultingProperty);

        // Then
        assertThat(value).isEqualTo("some-table");
    }

    @Test
    void shouldThrowRuntimeExceptionIfConfiguredWithNonInstanceOrNonTableProperty() {
        // Given
        SleeperProperty sleeperProperty = new DummySleeperProperty();

        // When / Then
        assertThatThrownBy(() -> DummyTableProperty.defaultedFrom(sleeperProperty))
                .hasMessageStartingWith("Unexpected default property type: ");
    }

    @Test
    void shouldApplyCustomComputeBehaviour() {
        // Given
        Iterator<String> values = Stream.iterate(1, i -> i + 1).map(i -> "" + i).iterator();
        DummyTableProperty property = DummyTableProperty.customCompute((value, instanceProperties, tableProperties) -> values.next());
        TableProperties tableProperties = new TableProperties(new InstanceProperties());

        // When
        String value1 = tableProperties.get(property);
        String value2 = tableProperties.get(property);
        String value3 = tableProperties.get(property);

        // Then
        assertThat(List.of(value1, value2, value3))
                .containsExactly("1", "2", "3");
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
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(properties);

        Properties differentProperties = new Properties();
        properties.setProperty("a", "c");
        InstanceProperties differentInstanceProperties = InstanceProperties.createWithoutValidation(differentProperties);

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
                        .getValidationPredicate() != property.getValidationPredicate()))
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

    @Test
    void shouldNameTablePropertiesConsistently() {
        assertThat(TableProperty.getAll())
                .extracting(TableProperty::getPropertyName)
                .allSatisfy(name -> assertThat(name).startsWith("sleeper.table."));
    }
}
