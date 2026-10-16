/*
 * Copyright 2022-2026 Crown Copyright
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.SleeperPropertiesValidationReporter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.SCHEMA;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;

class TablePropertiesIteratorConfigTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new LongType()))
            .valueFields(new Field("value", new LongType()))
            .build();
    private final TableProperties properties = createTestTableProperties(instanceProperties, schema);

    @ParameterizedTest
    @ValueSource(strings = {"unknown(value)", "sum(missing)", "sum(key)", "sum(value),sum(value)"})
    void shouldRejectInvalidAggregationAgainstItsProperty(String config) {
        // Given
        properties.set(AGGREGATION_CONFIG, config);
        SleeperPropertiesValidationReporter reporter = new SleeperPropertiesValidationReporter();

        // When
        properties.validate(reporter);

        // Then
        assertThat(reporter.isValid(AGGREGATION_CONFIG)).isFalse();
        assertThat(reporter.isValid(FILTERING_CONFIG)).isTrue();
        assertThat(reporter.isValid(SCHEMA)).isTrue();
        assertThat(properties.isValid()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown(key,1000)", "ageOff(key,invalid)", "ageOff(key)"})
    void shouldRejectInvalidFilteringAgainstItsProperty(String config) {
        // Given
        properties.set(FILTERING_CONFIG, config);
        SleeperPropertiesValidationReporter reporter = new SleeperPropertiesValidationReporter();

        // When
        properties.validate(reporter);

        // Then
        assertThat(reporter.isValid(FILTERING_CONFIG)).isFalse();
        assertThat(reporter.isValid(AGGREGATION_CONFIG)).isTrue();
        assertThat(reporter.isValid(SCHEMA)).isTrue();
    }

    @Test
    void shouldReportBothInvalidConfigs() {
        // Given
        properties.set(AGGREGATION_CONFIG, "unknown(value)");
        properties.set(FILTERING_CONFIG, "unknown(key,1000)");

        // When / Then
        assertThatThrownBy(properties::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining(AGGREGATION_CONFIG.getPropertyName(), FILTERING_CONFIG.getPropertyName());
    }

    @Test
    void shouldAcceptValidConfigWithoutLoadingCustomIterator() {
        // Given
        properties.set(AGGREGATION_CONFIG, "sum(value)");
        properties.set(FILTERING_CONFIG, "ageOff(key,1000)");
        properties.set(ITERATOR_CLASS_NAME, "example.NotOnClasspath");

        // When / Then
        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "sum(value)"})
    void shouldAcceptUnsetOrValidConfig(String aggregation) {
        // Given
        properties.set(AGGREGATION_CONFIG, aggregation);

        // When / Then
        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    void shouldRejectInvalidAggregationWhenCreatingTable() {
        // Given
        properties.set(AGGREGATION_CONFIG, "sum(missing)");

        // When / Then
        assertThatThrownBy(() -> TableProperties.createAndValidate(instanceProperties, properties.getProperties()))
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining(AGGREGATION_CONFIG.getPropertyName());
    }

    @Test
    void shouldRejectInvalidFilteringWhenUpdatingTable() {
        // Given
        TableProperties updated = TableProperties.copyOf(properties);
        updated.set(FILTERING_CONFIG, "ageOff(key,invalid)");

        // When / Then
        assertThatThrownBy(() -> properties.resetAndValidate(updated.getProperties()))
                .isInstanceOf(SleeperPropertiesInvalidException.class)
                .hasMessageContaining(FILTERING_CONFIG.getPropertyName());
    }

    @Test
    void shouldReportInvalidSchemaWithoutAnAggregationFailure() {
        // Given
        properties.set(SCHEMA, "{}");
        properties.set(AGGREGATION_CONFIG, "sum(value)");
        SleeperPropertiesValidationReporter reporter = new SleeperPropertiesValidationReporter();

        // When
        properties.validate(reporter);

        // Then
        assertThat(reporter.isValid(SCHEMA)).isFalse();
        assertThat(reporter.isValid(AGGREGATION_CONFIG)).isTrue();
    }
}
