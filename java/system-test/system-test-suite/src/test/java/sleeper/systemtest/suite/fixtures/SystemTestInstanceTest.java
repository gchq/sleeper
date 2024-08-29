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

package sleeper.systemtest.suite.fixtures;

import org.junit.jupiter.api.Test;

import sleeper.configuration.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.CommonProperty;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.testutil.SystemTestParametersTestHelper.parametersBuilder;

public class SystemTestInstanceTest {

    private final List<SystemTestInstanceConfiguration> instances = readInstances();

    @Test
    void shouldFindInstances() {
        assertThat(instances).isNotEmpty();
    }

    @Test
    void shouldProduceValidInstanceIds() {
        SystemTestParameters parameters = parametersBuilder()
                .shortTestId("mvn-10110302") // Contains month, day, hour, minute
                .build();
        assertThat(instances)
                .extracting(instance -> parameters.buildInstanceId(instance.getShortName()))
                .allMatch(CommonProperty.ID.validationPredicate());
    }

    @Test
    void shouldForceStateStoreClassname() {
        SystemTestParameters parameters = parametersBuilder()
                .forceStateStoreClassname("test-class")
                .build();

        assertThat(instances)
                .extracting(config -> config.buildDeployConfig(parameters))
                .flatExtracting(DeployInstanceConfiguration::getTableProperties)
                .extracting(tableProperties -> tableProperties.get(TableProperty.STATESTORE_CLASSNAME))
                .hasSize(instances.size())
                .containsOnly("test-class");
    }

    private List<SystemTestInstanceConfiguration> readInstances() {
        return Stream.of(SystemTestInstance.class.getDeclaredFields())
                .filter(field -> field.getType() == SystemTestInstanceConfiguration.class)
                .map(field -> {
                    try {
                        return (SystemTestInstanceConfiguration) field.get(null);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toUnmodifiableList());
    }
}
