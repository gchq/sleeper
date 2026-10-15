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
package sleeper.bulkimport.core.configuration;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_IMAGE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DNS_SUFFIX;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;

class SparkConfigurationUtilsTest {

    @Test
    void shouldUseDeployedImageForSparkPods() {
        // Given
        InstanceProperties properties = instanceProperties();
        String customImage = "registry.example.com/custom/spark@sha256:" + "a".repeat(64);
        properties.set(BULK_IMPORT_EKS_IMAGE, customImage);

        // When / Then
        assertThat(SparkConfigurationUtils.getSparkConfigurationForEKSFromInstanceProperties(properties))
                .containsEntry("spark.kubernetes.container.image", customImage);
    }

    @Test
    void shouldRetainImageNameForOlderDeployments() {
        // Given
        InstanceProperties properties = instanceProperties();

        // When / Then
        assertThat(SparkConfigurationUtils.getSparkConfigurationForEKSFromInstanceProperties(properties))
                .containsEntry("spark.kubernetes.container.image",
                        "test-account.dkr.ecr.test-region.amazonaws.com/test-instance/bulk-import-runner:1.2.3");
    }
    private InstanceProperties instanceProperties() {
        InstanceProperties properties = new InstanceProperties();
        properties.set(ACCOUNT, "test-account");
        properties.set(REGION, "test-region");
        properties.set(DNS_SUFFIX, "amazonaws.com");
        properties.set(VERSION, "1.2.3");
        properties.set(ECR_REPOSITORY_PREFIX, "test-instance");
        return properties;
    }

}
