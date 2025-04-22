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

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.REGION;

public class DockerDeploymentTest {

    @Test
    void shouldComputeRepositoryNameFromPrefix() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(ECR_REPOSITORY_PREFIX, "test");

        // When
        String repositoryName = DockerDeployment.INGEST.getEcrRepositoryName(properties);

        // Then
        assertThat(repositoryName).isEqualTo("test/ingest");
    }

    @Test
    void shouldComputeRepositoryNameFromInstanceIdWhenPrefixIsNotSet() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(ID, "test-instance");

        // When
        String repositoryName = DockerDeployment.INGEST.getEcrRepositoryName(properties);

        // Then
        assertThat(repositoryName).isEqualTo("test-instance/ingest");
    }

    @Test
    void shouldComputeRepositoryNameForAnotherDeployment() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(ECR_REPOSITORY_PREFIX, "test");

        // When
        String repositoryName = DockerDeployment.EKS_BULK_IMPORT.getEcrRepositoryName(properties);

        // Then
        assertThat(repositoryName).isEqualTo("test/bulk-import-runner");
    }

    @Test
    void shouldComputeDockerImageWithTag() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(ACCOUNT, "1234");
        properties.set(REGION, "global");
        properties.set(VERSION, "1.2.3");
        properties.set(ECR_REPOSITORY_PREFIX, "test");

        // When
        String imageName = DockerDeployment.INGEST.getDockerImageName(properties);

        // Then
        assertThat(imageName).isEqualTo("1234.dkr.ecr.global.amazonaws.com/test/ingest:1.2.3");
    }

}
