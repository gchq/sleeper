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
package sleeper.systemtest.configuration;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.SleeperPropertiesInvalidException;
import sleeper.configuration.properties.validation.IngestQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_ENTRIES_RANDOM_LIST;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_ENTRIES_RANDOM_MAP;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_RANDOM_LONG;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_LONG;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.configuration.SystemTestProperty.RANDOM_BYTE_ARRAY_LENGTH;
import static sleeper.systemtest.configuration.SystemTestProperty.RANDOM_STRING_LENGTH;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

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
    void shouldFailValidationWhenMandatorySystemTestPropertyNotSet() {
        // Given
        SystemTestProperties properties = validProperties();
        properties.unset(SYSTEM_TEST_REPO);

        // When / Then
        assertThatThrownBy(properties::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class);
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

    private SystemTestProperties validProperties() {
        SystemTestProperties properties = new SystemTestProperties(createTestInstanceProperties().getProperties());
        properties.setNumber(NUMBER_OF_WRITERS, 1);
        properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 1);
        properties.setEnum(INGEST_MODE, DIRECT);
        properties.set(SYSTEM_TEST_REPO, "test-repo");
        properties.set(SYSTEM_TEST_CLUSTER_ENABLED, "false");
        properties.set(INGEST_QUEUE, IngestQueue.STANDARD_INGEST.toString());
        properties.setNumber(NUMBER_OF_INGESTS_PER_WRITER, 1);
        properties.setNumber(MIN_RANDOM_INT, 0);
        properties.setNumber(MAX_RANDOM_INT, 100000000);
        properties.setNumber(MIN_RANDOM_LONG, 0);
        properties.setNumber(MAX_RANDOM_LONG, 100000000);
        properties.setNumber(RANDOM_STRING_LENGTH, 10);
        properties.setNumber(RANDOM_BYTE_ARRAY_LENGTH, 10);
        properties.setNumber(MAX_ENTRIES_RANDOM_MAP, 10);
        properties.setNumber(MAX_ENTRIES_RANDOM_LIST, 10);
        return properties;
    }
}
