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
import sleeper.build.status.github.GitHubProvider;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckGitHubStatusTest {

    @Test
    public void shouldPassWhenSingleChunkSuccessful() throws Exception {
        GitHubHead branch = GitHubHead.builder()
                .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
                .build();
        ProjectChunk chunk = ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build();
        ProjectConfiguration configuration = ProjectConfiguration.builder()
                .token("test-token").head(branch)
                .chunks(Collections.singletonList(chunk))
                .build();
        GitHubProvider gitHub = mock(GitHubProvider.class);
        when(gitHub.workflowStatus(branch, chunk)).thenReturn(ChunkStatus.chunk(chunk).commitSha("test-sha").success());

        ChunksStatus status = configuration.checkStatus(gitHub);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Build is for current commit");
    }

    @Test
    public void shouldRetryWhenSingleChunkInProgressOnOldSha() throws Exception {
        GitHubHead branch = GitHubHead.builder()
                .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
                .build();
        ProjectChunk chunk = ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build();
        ProjectConfiguration configuration = ProjectConfiguration.builder()
                .token("test-token").head(branch)
                .chunks(Collections.singletonList(chunk))
                .retrySeconds(0)
                .build();
        ChunkStatus status1 = ChunkStatus.chunk(chunk).commitSha("old-sha").inProgress();
        ChunkStatus status2 = ChunkStatus.chunk(chunk).commitSha("old-sha").success();
        GitHubProvider gitHub = mock(GitHubProvider.class);
        when(gitHub.workflowStatus(branch, chunk)).thenReturn(status1);
        when(gitHub.recheckRun(branch, status1)).thenReturn(status2);

        ChunksStatus status = configuration.checkStatus(gitHub);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Commit: old-sha");
    }

    @Test
    public void shouldStopRetryingAfterSpecifiedTimes() throws Exception {
        GitHubHead branch = GitHubHead.builder()
                .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
                .build();
        ProjectChunk chunk = ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build();
        ProjectConfiguration configuration = ProjectConfiguration.builder()
                .token("test-token").head(branch)
                .chunks(Collections.singletonList(chunk))
                .retrySeconds(0).maxRetries(1)
                .build();
        ChunkStatus status1 = ChunkStatus.chunk(chunk).commitSha("old-sha").inProgress();
        GitHubProvider gitHub = mock(GitHubProvider.class);
        when(gitHub.workflowStatus(branch, chunk)).thenReturn(status1);
        when(gitHub.recheckRun(branch, status1)).thenReturn(status1);

        ChunksStatus status = configuration.checkStatus(gitHub);
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportLines()).containsExactly("",
                "Common: in_progress",
                "Commit: old-sha");

        verify(gitHub, times(1)).recheckRun(branch, status1);
    }
}
