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

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemTestRoleTest {

    private final Map<String, String> contextVariables = new HashMap<>();
    private final Map<String, String> environmentVariables = new HashMap<>();

    @Test
    void shouldSetNoSystemTestRole() {
        // Given
        InstanceProperties properties = new InstanceProperties();

        // When
        SystemTestRole.addSystemTestRole(properties, tryGetContext(), getenv());

        // Then
        assertThat(properties).isEqualTo(new InstanceProperties());
    }

    @Test
    void shouldSetSystemTestRoleFromContext() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        contextVariables.put("role", "test-role");

        // When
        SystemTestRole.addSystemTestRole(properties, tryGetContext(), getenv());

        // Then
        InstanceProperties expected = new InstanceProperties();
        SystemTestRole.SYSTEM_TEST_ROLE_PROPERTIES.forEach(property -> expected.set(property, "test-role"));
        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldSetSystemTestRoleFromEnvironment() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        environmentVariables.put("SLEEPER_SYSTEM_TEST_ROLE", "test-role");

        // When
        SystemTestRole.addSystemTestRole(properties, tryGetContext(), getenv());

        // Then
        InstanceProperties expected = new InstanceProperties();
        SystemTestRole.SYSTEM_TEST_ROLE_PROPERTIES.forEach(property -> expected.set(property, "test-role"));
        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldAddRoleToExistingList() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        SystemTestRole.SYSTEM_TEST_ROLE_PROPERTIES.forEach(
                property -> properties.set(property, "before"));
        contextVariables.put("role", "add");

        // When
        SystemTestRole.addSystemTestRole(properties, tryGetContext(), getenv());

        // Then
        InstanceProperties expected = new InstanceProperties();
        SystemTestRole.SYSTEM_TEST_ROLE_PROPERTIES.forEach(
                property -> expected.setList(property, List.of("before", "add")));
        assertThat(properties).isEqualTo(expected);
    }

    private Function<String, String> tryGetContext() {
        return contextVariables::get;
    }

    private Function<String, String> getenv() {
        return environmentVariables::get;
    }

}
