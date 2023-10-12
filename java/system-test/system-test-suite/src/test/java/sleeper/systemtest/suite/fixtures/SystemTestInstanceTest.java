/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.fixtures;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.CommonProperty;
import sleeper.systemtest.drivers.instance.SystemTestParameters;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemTestInstanceTest {

    @Test
    void shouldProduceValidInstanceIds() {
        SystemTestParameters parameters = SystemTestParameters.builder()
                .shortTestId("mvn-10110302") // Contains month, day, hour, minute
                .build();
        assertThat(SystemTestInstance.values())
                .extracting(instance -> parameters.buildInstanceId(instance.getIdentifier()))
                .allMatch(CommonProperty.ID.validationPredicate());
    }
}
