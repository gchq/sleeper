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
package sleeper.cdk;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class UtilsResourceNameTest {

    @Test
    void shouldBuildResourceNameFromInstanceProperties() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ID, "test-instance");

        // When
        String name = Utils.buildInstanceFunctionName(properties, "test-resource");

        // Then
        assertThat(name).isEqualTo("sleeper-test-instance-test-resource");
    }

    @Test
    void shouldConvertInstanceIdToLowerCase() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ID, "AnInstance");

        // When
        String name = Utils.buildInstanceFunctionName(properties, "resource");

        // Then
        assertThat(name).isEqualTo("sleeper-aninstance-resource");
    }

    @Test
    void shouldRefuseResourceNameWhichDoesNotFitWithLongestInstanceId() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ID, "AnInstance");
        String name = "a-name-too-long-to-fit-within-resource-name-alongside-instance-id";

        // When / Then
        assertThatThrownBy(() -> Utils.buildInstanceFunctionName(properties, name))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
