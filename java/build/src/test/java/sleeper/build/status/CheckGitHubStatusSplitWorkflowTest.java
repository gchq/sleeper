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
package sleeper.build.status;

import org.junit.jupiter.api.Test;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.InMemoryGitHubWorkflowRuns;
import sleeper.build.github.TestGitHubHead;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckGitHubStatusSplitWorkflowTest {

    private final GitHubHead branch = TestGitHubHead.example();
    private final ProjectChunk chunk = ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build();
    private final InMemoryGitHubWorkflowRuns workflowRuns = new InMemoryGitHubWorkflowRuns(branch, chunk.getWorkflow());

    private CheckGitHubStatusConfig.Builder configurationBuilder() {
        return CheckGitHubStatusConfig.builder()
                .token("test-token").head(branch)
                .chunks(Collections.singletonList(chunk));
    }

    @Test
    public void shouldPassWhenSingleChunkSuccessful() throws Exception {
        CheckGitHubStatusConfig configuration = configurationBuilder().build();
        workflowRuns.setLatestRun(GitHubWorkflowRun.withCommitSha("test-sha").runId(123L).success());

        ChunkStatuses status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Build is for current commit");
    }

    @Test
    public void shouldRetryWhenSingleChunkInProgressOnOldSha() throws Exception {
        CheckGitHubStatusConfig configuration = configurationBuilder().retrySeconds(0).build();
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("old-sha").runId(123L);
        workflowRuns.setLatestRunAndRecheck(runBuilder.inProgress(), runBuilder.success());

        ChunkStatuses status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Commit: old-sha");
    }

    @Test
    public void shouldRetryMultipleTimes() throws Exception {
        CheckGitHubStatusConfig configuration = configurationBuilder().retrySeconds(0).maxRetries(10).build();
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("old-sha").runId(12L);
        workflowRuns.setLatestRunAndRechecks(runBuilder.inProgress(), runBuilder.inProgress(), runBuilder.success());

        ChunkStatuses status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportLines()).containsExactly("",
                "Common: completed, success",
                "Commit: old-sha");

        workflowRuns.verifyExactlySpecifiedRechecksDone();
    }

    @Test
    public void shouldStopRetryingAfterSpecifiedTimes() throws Exception {
        CheckGitHubStatusConfig configuration = configurationBuilder().retrySeconds(0).maxRetries(1).build();
        GitHubWorkflowRun run = GitHubWorkflowRun.withCommitSha("old-sha").runId(12L).inProgress();
        workflowRuns.setLatestRunAndRecheck(run, run);

        ChunkStatuses status = workflowRuns.checkStatus(configuration);
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportLines()).containsExactly("",
                "Common: in_progress",
                "Commit: old-sha");

        workflowRuns.verifyExactlySpecifiedRechecksDone();
    }
}
