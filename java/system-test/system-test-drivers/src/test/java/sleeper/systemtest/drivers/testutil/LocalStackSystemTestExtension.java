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
package sleeper.systemtest.drivers.testutil;

import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.localstack.test.SleeperLocalStackContainer;
import sleeper.systemtest.dsl.extension.SleeperSystemTestExtension;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.testutil.SystemTestParametersTestHelper;

public class LocalStackSystemTestExtension extends SleeperSystemTestExtension {

    private static final LocalStackContainer CONTAINER = SleeperLocalStackContainer.INSTANCE;
    private static final SystemTestDeploymentContext CONTEXT = new SystemTestDeploymentContext(
            SystemTestParametersTestHelper.parametersBuilder()
                    .shortTestId("localstack")
                    .region(CONTAINER.getRegion())
                    .build(),
            LocalStackSystemTestDrivers.fromContainer(CONTAINER));

    private LocalStackSystemTestExtension() {
        super(CONTEXT);
    }

}
