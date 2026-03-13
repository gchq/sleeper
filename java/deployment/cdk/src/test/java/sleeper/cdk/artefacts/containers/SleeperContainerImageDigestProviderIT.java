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
package sleeper.cdk.artefacts.containers;

import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrTestBase;
import sleeper.core.properties.model.OptionalStack;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.buildImageCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.dockerLoginToEcrCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.pushImageCommand;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class SleeperContainerImageDigestProviderIT extends UploadDockerImagesToEcrTestBase {

    //private final String repositoryName = UUID.randomUUID().toString();
    //private final InstanceProperties instanceProperties = createInstanceProperties();
    protected final Map<Path, String> files = new HashMap<>();
    DeployConfiguration deployConfig = DeployConfiguration.fromLocalBuild();

    @Test
    void shouldGetLatestVersionOfAnImage() throws Exception {

        // Given
        properties.setEnum(OPTIONAL_STACKS, OptionalStack.IngestStack);

        // When
        uploadForDeployment(dockerDeploymentImageConfig());

        // Then
        String expectedTag = "123.dkr.ecr.test-region.amazonaws.com/test-instance/ingest:1.0.0";
        assertThat(commandsThatRan).containsExactly(
                dockerLoginToEcrCommand(),
                buildImageCommand(expectedTag, "./docker/ingest"),
                pushImageCommand(expectedTag));
    }

    // private InstanceProperties createInstanceProperties() {
    //     InstanceProperties properties = createTestInstanceProperties();
    //     return properties;
    // }

    // private SleeperContainerImageDigestProvider images() {
    //     return SleeperContainerImageDigestProvider.from(ecrClient, properties);
    // }

    @Override
    protected UploadDockerImagesToEcr uploader() {
        return new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .deployConfig(deployConfig)
                        .commandRunner(commandRunner)
                        .copyFile((source, target) -> files.put(target, files.get(source)))
                        .baseDockerDirectory(Path.of("./docker")).jarsDirectory(Path.of("./jars"))
                        .version("1.0.0")
                        .build(),
                ecrClient, "123", "test-region");
    }
}
