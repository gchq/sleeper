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

package sleeper.systemtest.suite.testutil;

import sleeper.systemtest.drivers.instance.AwsSystemTestParameters;
import sleeper.systemtest.drivers.util.AwsSystemTestDrivers;
import sleeper.systemtest.dsl.extension.SleeperSystemTestExtension;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;

public class AwsSleeperSystemTestExtension extends SleeperSystemTestExtension {

    private static final SystemTestDeploymentContext CONTEXT = createContext();

    public AwsSleeperSystemTestExtension() {
        super(CONTEXT);
    }

    private static SystemTestDeploymentContext createContext() {
        SystemTestDeploymentContext context = new SystemTestDeploymentContext(
                AwsSystemTestParameters.loadFromSystemProperties(), new AwsSystemTestDrivers());
        try {
            context.deployedResources().deployIfMissing();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        return context;
    }
}
