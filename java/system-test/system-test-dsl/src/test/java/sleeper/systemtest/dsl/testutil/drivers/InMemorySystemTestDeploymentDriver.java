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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class InMemorySystemTestDeploymentDriver implements SystemTestDeploymentDriver {

    private SystemTestStandaloneProperties properties;

    @Override
    public void saveProperties(SystemTestStandaloneProperties properties) {
        if (this.properties == null) {
            throw new IllegalStateException("System test not yet deployed");
        }
        this.properties = properties;
    }

    @Override
    public SystemTestStandaloneProperties loadProperties() {
        if (properties == null) {
            throw new IllegalStateException("System test not yet deployed");
        }
        return properties;
    }

    @Override
    public boolean deployIfNotPresent(SystemTestStandaloneProperties properties) {
        if (properties == null) {
            return false;
        } else {
            properties.set(SYSTEM_TEST_BUCKET_NAME, "in-memory-system-test-bucket");
            this.properties = properties;
            return true;
        }
    }

    @Override
    public void redeploy(SystemTestStandaloneProperties properties) {
        this.properties = properties;
    }
}
