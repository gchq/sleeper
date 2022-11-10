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
import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectConfiguration;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.TestGitHubWorkflowRuns;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckGitHubStatusTest {

    private final TestGitHubWorkflowRuns workflowRuns = new TestGitHubWorkflowRuns();

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
        workflowRuns.setLatestRun(branch, chunk.getWorkflow(),
                GitHubWorkflowRun.withCommitSha("test-sha").runId(123L).success());

        ChunksStatus status = workflowRuns.checkStatus(configuration);
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
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("old-sha").runId(123L);
        workflowRuns.setLatestRunAndRecheck(branch, chunk.getWorkflow(),
                runBuilder.inProgress(), runBuilder.success());

        ChunksStatus status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Commit: old-sha");
    }

    @Test
    public void shouldRetryMultipleTimes() throws Exception {
        GitHubHead branch = GitHubHead.builder()
                .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
                .build();
        ProjectChunk chunk = ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build();
        ProjectConfiguration configuration = ProjectConfiguration.builder()
                .token("test-token").head(branch)
                .chunks(Collections.singletonList(chunk))
                .retrySeconds(0).maxRetries(10)
                .build();
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("old-sha").runId(12L);
        workflowRuns.setLatestRunAndRecheck(branch, chunk.getWorkflow(),
                runBuilder.inProgress(), runBuilder.inProgress(), runBuilder.success());

        ChunksStatus status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Commit: old-sha");

        workflowRuns.verifyTimesRechecked(branch, runBuilder.inProgress(), 2);
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
        GitHubWorkflowRun run = GitHubWorkflowRun.withCommitSha("old-sha").runId(12L).inProgress();
        workflowRuns.setLatestRunAndRecheck(branch, chunk.getWorkflow(), run);

        ChunksStatus status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportLines()).containsExactly("",
                "Common: in_progress",
                "Commit: old-sha");

        workflowRuns.verifyTimesRechecked(branch, run, 1);
    }
}
