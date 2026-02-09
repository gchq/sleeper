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
package sleeper.systemtest.configuration;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.SleeperPropertiesInvalidException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_ROWS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REGION;

class SystemTestPropertiesTest {

    @Test
    void shouldPassValidationWithValidProperties() {
        // Given
        SystemTestProperties properties = validProperties();

        // When / Then
        assertThatCode(properties::validate)
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFailValidationWhenIngestModeIsNotRecognised() {
        // Given
        SystemTestProperties properties = validProperties();
        properties.set(INGEST_MODE, "invalid");

        // When / Then
        assertThatThrownBy(properties::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }

    @Test
    void shouldFailValidationWhenInstancePropertyIsInvalid() {
        // Given
        SystemTestProperties properties = validProperties();
        properties.set(MAXIMUM_CONNECTIONS_TO_S3, "-1");

        // When / Then
        assertThatThrownBy(properties::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }

    @Test
    void shouldFindNoUnknownProperties() {
        // Given
        SystemTestProperties properties = validProperties();

        // When / Then
        assertThat(properties.getUnknownProperties())
                .isEmpty();
    }

    @Test
    void shouldAcceptDefaultValueForSystemPropertyWhenValueNotSet() {
        // Given
        SystemTestProperties properties = validProperties();

        // When / Then
        assertThat(properties.get(MIN_RANDOM_INT)).isEqualTo("0");
    }

    @Test
    void shouldReturnNullValueWhenAttemptingToRetrievePropertyNotSet() {
        // Given
        SystemTestProperties properties = validProperties();

        // When / Then
        assertThat(properties.get(SYSTEM_TEST_REGION)).isNull();
    }

    @Test
    void shouldValidateThatSystemPropertyCanBeSetForStandaloneProperties() {
        // Given
        SystemTestProperties properties = validProperties();

        // When
        properties.setNumber(NUMBER_OF_WRITERS, 10);

        // Then
        assertThat(properties.getInt(NUMBER_OF_WRITERS)).isEqualTo(10);
    }

    private SystemTestProperties validProperties() {
        SystemTestProperties properties = new SystemTestProperties(createTestInstanceProperties().getProperties());
        properties.setNumber(NUMBER_OF_WRITERS, 1);
        properties.setNumber(NUMBER_OF_ROWS_PER_INGEST, 1);
        properties.setEnum(INGEST_MODE, DIRECT);
        return properties;
    }
}
