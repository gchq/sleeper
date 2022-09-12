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
package sleeper.build.status;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectConfigurationTest {

    @Test
    public void shouldLoadPropertiesForSeveralChunks() throws Exception {
        Properties properties = TestProperties.example("github-example.properties");
        ProjectConfiguration gitHub = ProjectConfiguration.from(properties);

        assertThat(gitHub).isEqualTo(
                ProjectConfiguration.builder()
                        .token("test-token")
                        .branch(GitHubBranch.builder()
                                .owner("test-owner").repository("test-repo").branch("test-branch")
                                .build())
                        .chunks(Arrays.asList(
                                ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build(),
                                ProjectChunk.chunk("data").name("Data").workflow("chunk-data.yaml").build()))
                        .build());
    }

    @Test
    public void shouldPassWhenSingleChunkSuccessful() {
        GitHubBranch branch = GitHubBranch.builder()
                .owner("test-owner").repository("test-repo").branch("test-branch")
                .build();
        ProjectChunk chunk = ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build();
        ProjectConfiguration configuration = ProjectConfiguration.builder()
                .token("test-token").branch(branch)
                .chunks(Collections.singletonList(chunk))
                .build();
        GitHubProvider gitHub = mock(GitHubProvider.class);
        when(gitHub.workflowStatus(branch, chunk)).thenReturn(ChunkStatus.chunk(chunk).success());

        ChunksStatus status = configuration.checkStatus(gitHub);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "Common: completed, success\n");
    }
}
