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

package sleeper.clients.admin;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.clients.deploy.GenerateInstanceProperties;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;

public class PropertiesDiffTest {
    @Test
    void shouldNotDetectDifferenceIfInstancePropertiesHaveNotChanged() {
        // Given
        InstanceProperties properties1 = createInstanceProperties("test-instance");
        InstanceProperties properties2 = createInstanceProperties("test-instance");

        // When
        PropertiesDiff<InstanceProperty> diff = new PropertiesDiff<>(properties1, properties2);

        // Then
        assertThat(diff.isChanged()).isFalse();
        assertThat(diff.getChanges()).isEmpty();
    }

    @Disabled("TODO")
    @Test
    void shouldDetectDifferenceIfPropertyHasBeenUpdatedInInstanceProperties() {
        // Given
        InstanceProperties properties1 = createInstanceProperties("test-instance");
        properties1.set(MAXIMUM_CONNECTIONS_TO_S3, "30");
        InstanceProperties properties2 = createInstanceProperties("test-instance");
        properties2.set(MAXIMUM_CONNECTIONS_TO_S3, "25");

        // When
        PropertiesDiff<InstanceProperty> diff = new PropertiesDiff<>(properties1, properties2);

        // Then
        assertThat(diff.isChanged()).isTrue();
        assertThat(diff.getChanges())
                .containsExactly(new PropertyDiff(MAXIMUM_CONNECTIONS_TO_S3, "30", "25"));
    }

    private InstanceProperties createInstanceProperties(String instanceId) {
        return GenerateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL)
                .instanceId(instanceId).vpcId("some-vpc").subnetId("some-subnet").build().generate();
    }
}
