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

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.USER_JARS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS;

public class DeployedSleeperInstanceRedeployTest {
    InstanceProperties found = new InstanceProperties();
    InstanceProperties toDeploy = new InstanceProperties();
    InstanceProperties updated = new InstanceProperties();

    @Test
    void shouldRedeployIfCdkDeployedPropertyChanged() {
        // Given
        found.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 1);
        toDeploy.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 20);

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isTrue();
        assertThat(updated).isEqualTo(toDeploy);
    }

    @Test
    void shouldRedeployIfCdkDeployedPropertyIsUnset() {
        // Given
        found.setNumber(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, 1);
        toDeploy.set(COMPACTION_COMMIT_BATCHING_WINDOW_IN_SECONDS, null);

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isTrue();
        assertThat(updated).isEqualTo(toDeploy);
    }

    @Test
    void shouldNotRedeployIfUneditablePropertyChanged() {
        // Given
        found.set(VPC_ID, "some-vpc");
        toDeploy.set(VPC_ID, "other-vpc");

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isFalse();
        assertThat(updated).isEqualTo(found);
    }

    @Test
    void shouldNotRedeployIfNonCdkDeployedPropertyChanged() {
        // Given
        found.set(USER_JARS, "some-jars");
        toDeploy.set(USER_JARS, "other-jars");

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isFalse();
        assertThat(updated).isEqualTo(found);
    }

    // Tags are an unordered map, so we can't compare values in the same way as other properties
    @Test
    void shouldNotRedeployIfTagsChanged() {
        // Given
        found.setTags(Map.of("Value", "Found"));
        toDeploy.setTags(Map.of("Value", "To Deploy"));

        // When / Then
        assertThat(isRedeployDueToPropertyChange()).isFalse();
        assertThat(updated).isEqualTo(found);
    }

    private boolean isRedeployDueToPropertyChange() {
        updated = InstanceProperties.copyOf(found);
        return DeployedSleeperInstance.isRedeployDueToPropertyChange(toDeploy, updated);
    }

}
