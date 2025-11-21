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
package sleeper.core.deploy;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.deploy.PopulatePropertiesTestHelper.createTestPopulateInstanceProperties;
import static sleeper.core.deploy.PopulatePropertiesTestHelper.testPopulateInstancePropertiesBuilder;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.PartitionSplittingProperty.DEFAULT_PARTITION_SPLIT_THRESHOLD;

public class PopulateInstancePropertiesTest {

    private InstanceProperties expectedInstanceProperties() {
        InstanceProperties expected = new InstanceProperties();
        //expected.setTags(Map.of("InstanceID", "test-instance"));
        expected.set(ID, "test-instance");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ACCOUNT, "test-account-id");
        expected.set(REGION, "test-region");
        return expected;
    }

    @Test
    void shouldPopulateInstanceProperties() {
        // Given/When
        InstanceProperties properties = createTestPopulateInstanceProperties().populate(new InstanceProperties());

        // Then
        assertThat(properties).isEqualTo(expectedInstanceProperties());
    }

    @Test
    void shouldGetDefaultTagsWhenNotProvidedAndNotSetInInstanceProperties() {
        // Given/When
        InstanceProperties properties = createTestPopulateInstanceProperties().populate(new InstanceProperties());

        // Then
        assertThat(properties.getTags())
                .isEqualTo(Map.of("InstanceID", "test-instance"));
    }

    @Test
    void shouldAddToExistingTagsWhenSetInInstanceProperties() {
        // Given/When
        InstanceProperties properties = new InstanceProperties();
        properties.setTags(Map.of("TestTag", "TestValue"));
        createTestPopulateInstanceProperties().populate(properties);

        // Then
        assertThat(properties.getTags())
                .isEqualTo(Map.of("TestTag", "TestValue",
                        "InstanceID", "test-instance"));
    }

    @Test
    void shouldSetExtraProperties() {
        // Given/When
        InstanceProperties properties = new InstanceProperties();
        testPopulateInstancePropertiesBuilder()
                .extraInstanceProperties(p -> p.setNumber(DEFAULT_PARTITION_SPLIT_THRESHOLD, 1000))
                .build().populate(properties);

        // Then
        assertThat(properties.getInt(DEFAULT_PARTITION_SPLIT_THRESHOLD))
                .isEqualTo(1000);
    }

}
