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
package sleeper.clients.deploy.properties;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.core.deploy.PopulateInstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class PopulateInstancePropertiesAwsIT extends LocalStackTestBase {

    @Test
    void shouldPopulateInstancePropertiesCorrectly() {
        // Given/When
        InstanceProperties properties = populateInstancePropertiesBuilder()
                .build().populate(new InstanceProperties());

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.setTags(Map.of("InstanceID", "test-instance"));
        expected.set(ID, "test-instance");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ACCOUNT, stsClient.getCallerIdentity().account());
        expected.set(REGION, localStackContainer.getRegion());

        assertThat(properties).isEqualTo(expected);
    }

    private PopulateInstanceProperties.Builder populateInstancePropertiesBuilder() {
        return PopulateInstancePropertiesAws.builder(stsClient, () -> Region.of(localStackContainer.getRegion()))
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet");
    }
}
