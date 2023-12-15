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

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.CommonProperty;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.systemtest.drivers.instance.SystemTestParameters;

import static org.assertj.core.api.Assertions.assertThat;

public class SystemTestInstanceTest {

    @Test
    void shouldProduceValidInstanceIds() {
        SystemTestParameters parameters = parametersBuilder()
                .shortTestId("mvn-10110302") // Contains month, day, hour, minute
                .build();
        assertThat(SystemTestInstance.values())
                .extracting(instance -> parameters.buildInstanceId(instance.getIdentifier()))
                .allMatch(CommonProperty.ID.validationPredicate());
    }

    @Test
    void shouldForceStateStoreClassname() {
        SystemTestParameters parameters = parametersBuilder()
                .forceStateStoreClassname("test-class")
                .build();

        assertThat(SystemTestInstance.values())
                .extracting(instance -> instance.getInstanceConfiguration(parameters))
                .flatExtracting(DeployInstanceConfiguration::getTableProperties)
                .extracting(tableProperties -> tableProperties.get(TableProperty.STATESTORE_CLASSNAME))
                .asList().hasSize(SystemTestInstance.values().length)
                .containsOnly("test-class");
    }

    private SystemTestParameters.Builder parametersBuilder() {
        return SystemTestParameters.builder()
                .shortTestId("test-id")
                .account("test-account")
                .region("test-region")
                .vpcId("test-vpc")
                .subnetIds("test-subnet")
                .findDirectories();
    }
}
