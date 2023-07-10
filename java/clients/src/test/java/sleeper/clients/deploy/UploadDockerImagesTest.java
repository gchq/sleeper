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

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.RunCommandTestHelper.commandRunOn;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.CommonProperties.ACCOUNT;
import static sleeper.configuration.properties.CompactionProperties.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.IngestProperties.ECR_INGEST_REPO;
import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.CommonProperties.OPTIONAL_STACKS;
import static sleeper.configuration.properties.CommonProperties.REGION;
import static sleeper.core.SleeperVersion.getVersion;

class UploadDockerImagesTest {

    @Test
    void shouldRunDockerUpload() throws Exception {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ID, "test-instance");
        properties.set(ACCOUNT, "123");
        properties.set(REGION, "test-region");
        properties.set(OPTIONAL_STACKS, "CompactionStack,GarbageCollectorStack");
        properties.set(ECR_INGEST_REPO, "ingest-repo");
        properties.set(ECR_COMPACTION_REPO, "compaction-repo");

        UploadDockerImages upload = UploadDockerImages.builder()
                .uploadDockerImagesScript(Path.of("./uploadDockerImages.sh"))
                .baseDockerDirectory(Path.of("./docker"))
                .instanceProperties(properties)
                .build();

        // When / Then
        assertThat(commandRunOnUpload(upload))
                .containsExactly("./uploadDockerImages.sh",
                        "test-instance",
                        "123.dkr.ecr.test-region.amazonaws.com",
                        getVersion(),
                        "CompactionStack,GarbageCollectorStack",
                        "./docker");
    }

    private String[] commandRunOnUpload(UploadDockerImages upload) throws IOException, InterruptedException {
        return commandRunOn(upload::upload);
    }
}
