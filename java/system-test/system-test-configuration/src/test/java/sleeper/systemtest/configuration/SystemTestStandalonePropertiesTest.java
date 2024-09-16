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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

class SystemTestStandalonePropertiesTest {

    @Test
    void shouldPassWithValidSystemTestStandaloneProperties() {
        // Given
        SystemTestStandaloneProperties properties = validStandaloneProperties();

        // When / Then
        assertThatCode(properties::validate)
                .doesNotThrowAnyException();
    }

    @Test
    void shouldReturnNullWhenStandalonePropertyWithDefaultNotSet() {
        // Given
        SystemTestStandaloneProperties properties = validStandaloneProperties();

        // When / Then
        assertThat(properties.get(SYSTEM_TEST_BUCKET_NAME)).isNull();
    }

    @Test
    void shouldReturnDefaultValueForPropertyWhenValueNotSet() {
        // Given
        SystemTestStandaloneProperties properties = validStandaloneProperties();

        // When
        properties.set(SYSTEM_TEST_CLUSTER_ENABLED, null);

        // Then
        assertThat(properties.get(SYSTEM_TEST_CLUSTER_ENABLED)).isEqualTo("true");
    }

    private SystemTestStandaloneProperties validStandaloneProperties() {
        SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties(createTestInstanceProperties().getProperties());
        properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 12);
        properties.set(SYSTEM_TEST_REPO, "test-repo");
        return properties;
    }
}
