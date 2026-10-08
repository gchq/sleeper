/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.artefacts.containers;

import org.junit.jupiter.api.Test;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DNS_SUFFIX;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;

class SleeperContainerImagesFromPropertiesTest {

    @Test
    void shouldUseInstanceImageNameAndVersion() {
        // Given
        InstanceProperties properties = createTestInstancePropertiesWithId("test-instance");
        SleeperContainerImages images = images(properties);

        // When / Then
        assertThat(images.getDockerImageName(DockerDeployment.EKS_BULK_IMPORT))
                .isEqualTo("test-account.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner:1.2.3");
    }

    @Test
    void shouldUseCustomRegistryProperties() {
        // Given
        InstanceProperties properties = createTestInstancePropertiesWithId("test-instance");
        properties.set(ACCOUNT, "123456789012");
        properties.set(REGION, "cn-north-1");
        properties.set(DNS_SUFFIX, "amazonaws.com.cn");
        properties.set(ECR_REPOSITORY_PREFIX, "custom/artefacts");
        properties.set(VERSION, "custom-version");
        SleeperContainerImages images = images(properties);

        // When / Then
        assertThat(images.getDockerImageName(DockerDeployment.EKS_BULK_IMPORT))
                .isEqualTo("123456789012.dkr.ecr.cn-north-1.amazonaws.com.cn/custom/artefacts/bulk-import-runner:custom-version");
    }

    private SleeperContainerImages images(InstanceProperties properties) {
        return new SleeperContainerImagesFromProperties(properties,
                new SleeperContainerImageDigestProvider((image, repository) -> {
                    throw new AssertionError("Image name lookup should not access ECR");
                }));
    }
}
