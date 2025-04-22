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
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class DockerDeploymentTest {

    @Test
    void shouldComputeRepositoryNameFromPrefix() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(ECR_REPOSITORY_PREFIX, "test");

        // When
        String repositoryName = DockerDeployment.getEcrRepositoryName(properties, DockerDeployment.INGEST_NAME);

        // Then
        assertThat(repositoryName).isEqualTo("test/ingest");
    }

    @Test
    void shouldComputeRepositoryNameFromInstanceIdWhenPrefixIsNotSet() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(ID, "test-instance");

        // When
        String repositoryName = DockerDeployment.getEcrRepositoryName(properties, DockerDeployment.INGEST_NAME);

        // Then
        assertThat(repositoryName).isEqualTo("test-instance/ingest");
    }

}
