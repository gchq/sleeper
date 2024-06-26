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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.dsl.SystemTestDrivers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

public class InstanceAdminDriversWithRefresh {
    private final InstanceProperties instanceProperties;
    private final AssumeAdminRoleDriver assumeRoleDriver;
    private final Duration expiryTime;
    private final Supplier<Instant> clock;
    private SystemTestDrivers instanceAdminDrivers;
    private Instant lastAdminDriverRefresh;

    public InstanceAdminDriversWithRefresh(
            InstanceProperties instanceProperties, AssumeAdminRoleDriver assumeRoleDriver) {
        this(instanceProperties, assumeRoleDriver, Duration.ofMinutes(10), Clock.systemUTC()::instant);
    }

    public InstanceAdminDriversWithRefresh(
            InstanceProperties instanceProperties, AssumeAdminRoleDriver assumeRoleDriver, Duration expiryTime, Supplier<Instant> clock) {
        this.instanceProperties = instanceProperties;
        this.assumeRoleDriver = assumeRoleDriver;
        this.expiryTime = expiryTime;
        this.clock = clock;
    }

    public SystemTestDrivers drivers() {
        Instant timeNow = clock.get();
        if (lastAdminDriverRefresh == null || timeNow.isAfter(lastAdminDriverRefresh.plus(expiryTime))) {
            lastAdminDriverRefresh = timeNow;
            instanceAdminDrivers = assumeRoleDriver.assumeAdminRole(instanceProperties);
        }
        return instanceAdminDrivers;
    }
}
