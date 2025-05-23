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

package sleeper.systemtest.dsl.testutil;

import sleeper.systemtest.dsl.extension.SleeperSystemTestExtension;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;

import static sleeper.systemtest.dsl.testutil.SystemTestParametersTestHelper.UNIT_TEST_PARAMETERS;

public class InMemorySystemTestExtension extends SleeperSystemTestExtension {

    private static final SystemTestDeploymentContext CONTEXT = new SystemTestDeploymentContext(
            UNIT_TEST_PARAMETERS, new InMemorySystemTestDrivers());

    public InMemorySystemTestExtension() {
        super(CONTEXT);
    }
}
