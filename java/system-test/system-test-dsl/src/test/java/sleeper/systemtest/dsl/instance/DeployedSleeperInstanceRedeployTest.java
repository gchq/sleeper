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
package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.PopulateInstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.dsl.testutil.InMemoryTestInstance;

import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.USER_JARS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.dsl.testutil.SystemTestParametersTestHelper.UNIT_TEST_PARAMETERS;

public class DeployedSleeperInstanceRedeployTest {
    SystemTestStandaloneProperties systemTestProperties = new SystemTestStandaloneProperties();
    InstanceProperties toDeploy = new InstanceProperties();
    SystemTestInstanceConfiguration instanceConfig = InMemoryTestInstance.usingSystemTestDefaultsWithProperties(toDeploy);
    InstanceProperties existing;
    InstanceProperties updated; // Set when redeploy is checked

    @BeforeEach
    void setUp() {
        systemTestProperties.set(SYSTEM_TEST_BUCKET_NAME, "system-test-bucket");
        // Note this step is delayed until here so that the system test bucket is applied as an ingest source
        existing = fakeDeploy();
    }

    @Test
    void shouldRedeployIfCdkDeployedPropertyChanged() {
        // Given
        existing.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 1);
        toDeploy.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 20);

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isTrue();
        assertThat(updated).isEqualTo(existingWithUpdate(
                properties -> properties.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 20)));
    }

    @Test
    void shouldRedeployIfCdkDeployedPropertyIsUnset() {
        // Given
        existing.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 1);
        toDeploy.set(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, null);

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isTrue();
        assertThat(updated).isEqualTo(existingWithUpdate(
                properties -> properties.unset(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS)));
    }

    @Test
    void shouldNotRedeployIfUneditablePropertyChanged() {
        // Given
        existing.set(VPC_ID, "some-vpc");
        toDeploy.set(VPC_ID, "other-vpc");

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isFalse();
        assertThat(updated).isEqualTo(existing);
    }

    @Test
    void shouldNotRedeployIfNonCdkDeployedPropertyChanged() {
        // Given
        existing.set(USER_JARS, "some-jars");
        toDeploy.set(USER_JARS, "other-jars");

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isFalse();
        assertThat(updated).isEqualTo(existing);
    }

    // Tags are an unordered map, so we can't compare values in the same way as other properties
    @Test
    void shouldNotRedeployIfTagsChanged() {
        // Given
        existing.setTags(Map.of("Value", "Found"));
        toDeploy.setTags(Map.of("Value", "To Deploy"));

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isFalse();
        assertThat(updated).isEqualTo(existing);
    }

    private InstanceProperties existingWithUpdate(Consumer<InstanceProperties> update) {
        InstanceProperties copy = InstanceProperties.copyOf(existing);
        update.accept(copy);
        return copy;
    }

    private boolean isRedeployDueToPropertyChange() {
        updated = InstanceProperties.copyOf(existing);
        return DeployedSleeperInstance.isRedeployDueToPropertyChange(buildDeployConfig(), updated);
    }

    private InstanceProperties fakeDeploy() {
        // Replicate system test behaviour
        InstanceProperties properties = buildDeployConfig().getInstanceProperties();
        // Replicate behaviour in DeployNewInstance
        return PopulateInstanceProperties.builder()
                .accountSupplier(() -> "test-account")
                .regionIdSupplier(() -> "test-region")
                .instanceId("test-instance")
                .vpcId("test-vpc")
                .subnetIds("test-subnet")
                .build()
                .populate(properties);
    }

    private DeployInstanceConfiguration buildDeployConfig() {
        return instanceConfig.buildDeployConfig(UNIT_TEST_PARAMETERS, systemTestProperties);
    }

}
