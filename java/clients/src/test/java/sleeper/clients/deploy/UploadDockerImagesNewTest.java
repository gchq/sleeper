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

package sleeper.clients.deploy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.EcrRepositoriesInMemory;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.RunCommandTestHelper.command;
import static sleeper.clients.testutil.RunCommandTestHelper.pipelinesRunOn;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

public class UploadDockerImagesNewTest {
    final EcrRepositoriesInMemory ecrClient = new EcrRepositoriesInMemory();
    final InstanceProperties properties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        properties.set(ID, "test-instance");
        properties.set(ACCOUNT, "123");
        properties.set(REGION, "test-region");
    }

    @Test
    void shouldRunDockerUploadWithIngestStack() throws Exception {
        // Given
        properties.set(OPTIONAL_STACKS, "IngestStack");

        // When
        List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

        // Then
        assertThat(pipelinesThatRan)
                .containsExactly(loginDockerPipeline());

        assertThat(ecrClient.getRepositoryNames())
                .containsExactlyInAnyOrder("test-instance/ingest");
    }

    @Test
    void shouldRunDockerUploadWithTwoStacks() throws IOException, InterruptedException {
        // Given
        properties.set(OPTIONAL_STACKS, "IngestStack,EksBulkImportStack");

        // When
        List<CommandPipeline> pipelinesThatRan = pipelinesRunOn(getUpload()::upload);

        // Then
        assertThat(pipelinesThatRan)
                .containsExactly(loginDockerPipeline());

        assertThat(ecrClient.getRepositoryNames())
                .containsExactlyInAnyOrder("test-instance/ingest", "test-instance/bulk-import-runner");
    }

    private CommandPipeline loginDockerPipeline() {
        return pipeline(command("aws", "ecr", "get-login-password", "--region", "test-region"),
                command("docker", "login", "--username", "AWS", "--password-stdin",
                        "123.dkr.ecr.test-region.amazonaws.com"));
    }

    private UploadDockerImagesNew getUpload() {
        return UploadDockerImagesNew.builder()
                .baseDockerDirectory(Path.of("./docker"))
                .instanceProperties(properties)
                .ecrClient(ecrClient)
                .build();
    }
}
