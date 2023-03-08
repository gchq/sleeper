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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.clients.deploy.GenerateInstanceProperties;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;

public class PropertiesDiffTest {

    @DisplayName("Compare instance properties")
    @Nested
    class CompareInstanceProperties {

        @Test
        void shouldDetectNoChanges() {
            // Given
            InstanceProperties before = createInstanceProperties("test-instance");
            InstanceProperties after = createInstanceProperties("test-instance");

            // When / Then
            assertThat(getChanges(before, after)).isEmpty();
        }

        @Test
        void shouldDetectPropertyHasBeenUpdated() {
            // Given
            InstanceProperties before = createInstanceProperties("test-instance");
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "30");
            InstanceProperties after = createInstanceProperties("test-instance");
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "50");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(valueChanged(MAXIMUM_CONNECTIONS_TO_S3, "30", "50"));
        }

        @Test
        void shouldDetectPropertyIsNewlySet() {
            // Given
            InstanceProperties before = createInstanceProperties("test-instance");
            InstanceProperties after = createInstanceProperties("test-instance");
            after.set(INGEST_SOURCE_BUCKET, "some-bucket");

            // When / Then
            assertThat(getChanges(before, after))
                    .containsExactly(newValue(INGEST_SOURCE_BUCKET, "some-bucket"));
        }

        // TODO differentiate between case when value is a default or not
    }

    private <T extends SleeperProperty> List<PropertyDiff> getChanges(SleeperProperties<T> before, SleeperProperties<T> after) {
        return new PropertiesDiff<>(before, after).getChanges();
    }

    private PropertyDiff valueChanged(SleeperProperty property, String before, String after) {
        return new PropertyDiff(property, before, after);
    }

    private PropertyDiff newValue(SleeperProperty property, String value) {
        return new PropertyDiff(property, null, value);
    }

    private InstanceProperties createInstanceProperties(String instanceId) {
        return GenerateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL)
                .instanceId(instanceId).vpcId("some-vpc").subnetId("some-subnet").build().generate();
    }
}
