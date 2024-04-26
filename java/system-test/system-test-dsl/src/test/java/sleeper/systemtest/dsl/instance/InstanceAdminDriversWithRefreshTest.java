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
package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.dsl.SystemTestDrivers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

public class InstanceAdminDriversWithRefreshTest {

    private static final Instant DEFAULT_TIME = Instant.parse("2024-04-24T10:16:00Z");
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final AssumeAdminRoleDriver assumeRoleDriver = mock(AssumeAdminRoleDriver.class);

    @Test
    void shouldAssumeInstanceAdminRoleWhenFirstRetrieved() {
        // Given
        SystemTestDrivers drivers = mock(SystemTestDrivers.class);
        when(assumeRoleDriver.assumeAdminRole(instanceProperties)).thenReturn(drivers);

        // When
        SystemTestDrivers foundDrivers = instanceAdmin().drivers();

        // Then
        assertThat(foundDrivers).isEqualTo(drivers);
        verify(assumeRoleDriver, times(1)).assumeAdminRole(instanceProperties);
        verifyNoMoreInteractions(assumeRoleDriver);
    }

    @Test
    void shouldNotAssumeRoleAgainWhenRetrievedTwiceWithoutExpiring() {
        // Given
        SystemTestDrivers drivers = mock(SystemTestDrivers.class);
        when(assumeRoleDriver.assumeAdminRole(instanceProperties)).thenReturn(drivers);

        // When
        InstanceAdminDriversWithRefresh instanceAdmin = instanceAdminWithExpiryAndCheckTimes(
                Duration.ofMinutes(5),
                Instant.parse("2024-04-24T10:20:00Z"),
                Instant.parse("2024-04-24T10:21:00Z"));
        SystemTestDrivers found1 = instanceAdmin.drivers();
        SystemTestDrivers found2 = instanceAdmin.drivers();

        // Then
        assertThat(found1).isEqualTo(drivers);
        assertThat(found2).isEqualTo(drivers);
        verify(assumeRoleDriver, times(1)).assumeAdminRole(instanceProperties);
        verifyNoMoreInteractions(assumeRoleDriver);
    }

    @Test
    void shouldAssumeRoleAgainWhenRetrievedAfterExpiry() {
        // Given
        SystemTestDrivers drivers1 = mock(SystemTestDrivers.class);
        SystemTestDrivers drivers2 = mock(SystemTestDrivers.class);
        when(assumeRoleDriver.assumeAdminRole(instanceProperties)).thenReturn(drivers1, drivers2);

        // When
        InstanceAdminDriversWithRefresh instanceAdmin = instanceAdminWithExpiryAndCheckTimes(
                Duration.ofMinutes(5),
                Instant.parse("2024-04-24T10:20:00Z"),
                Instant.parse("2024-04-24T10:26:00Z"));
        SystemTestDrivers found1 = instanceAdmin.drivers();
        SystemTestDrivers found2 = instanceAdmin.drivers();

        // Then
        assertThat(found1).isEqualTo(drivers1);
        assertThat(found2).isEqualTo(drivers2);
        verify(assumeRoleDriver, times(2)).assumeAdminRole(instanceProperties);
        verifyNoMoreInteractions(assumeRoleDriver);
    }

    private InstanceAdminDriversWithRefresh instanceAdmin() {
        return instanceAdminWithExpiryAndClock(Duration.ofMinutes(50), () -> DEFAULT_TIME);
    }

    private InstanceAdminDriversWithRefresh instanceAdminWithExpiryAndCheckTimes(
            Duration expiryTime, Instant... checkTimes) {
        return instanceAdminWithExpiryAndClock(expiryTime, List.of(checkTimes).iterator()::next);
    }

    private InstanceAdminDriversWithRefresh instanceAdminWithExpiryAndClock(
            Duration expiryTime, Supplier<Instant> clock) {
        return new InstanceAdminDriversWithRefresh(instanceProperties, assumeRoleDriver, expiryTime, clock);
    }
}
