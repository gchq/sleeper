/*
 * Copyright 2022 Crown Copyright
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
package sleeper.build.chunks;

import org.junit.Test;
import sleeper.build.github.TestGitHubHead;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class ProjectConfigurationTest {

    @Test
    public void shouldLoadFromYamlAndGitHubProperties() {
        ProjectConfiguration configuration = ProjectConfiguration.fromGitHubAndChunks(
                TestProperties.example("examples/github.properties"),
                TestChunks.example("examples/chunks.yaml"));

        assertThat(configuration).isEqualTo(
                ProjectConfiguration.builder()
                        .token("test-token")
                        .head(TestGitHubHead.example())
                        .chunks(Arrays.asList(
                                ProjectChunk.chunk("bulk-import").name("Bulk Import")
                                        .workflow("chunk-bulk-import.yaml").modulesArray(
                                                "bulk-import/bulk-import-common",
                                                "bulk-import/bulk-import-starter",
                                                "bulk-import/bulk-import-runner")
                                        .build(),
                                ProjectChunk.chunk("common").name("Common")
                                        .workflow("chunk-common.yaml").modulesArray(
                                                "core", "configuration")
                                        .build(),
                                ProjectChunk.chunk("ingest").name("Ingest")
                                        .workflow("chunk-ingest.yaml").modulesArray("ingest").build()))
                        .build());
    }
}
