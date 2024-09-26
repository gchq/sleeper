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

package sleeper.systemtest.dsl.testutil;

import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

public class SystemTestParametersTestHelper {

    private SystemTestParametersTestHelper() {
    }

    public static final SystemTestParameters UNIT_TEST_PARAMETERS = parametersBuilder().build();

    public static SystemTestParameters.Builder parametersBuilder() {
        return SystemTestParameters.builder()
                .shortTestId("test-id")
                .account("test-account")
                .region("test-region")
                .vpcId("test-vpc")
                .subnetIds("test-subnet")
                .systemTestStandalonePropertiesTemplate(new SystemTestStandaloneProperties())
                .findDirectories();
    }
}
