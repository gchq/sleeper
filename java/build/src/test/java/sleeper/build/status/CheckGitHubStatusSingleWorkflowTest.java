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

import sleeper.build.chunks.ProjectChunks;
import sleeper.build.chunks.TestChunks;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.InMemoryGitHubWorkflowRuns;
import sleeper.build.github.TestGitHubHead;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckGitHubStatusSingleWorkflowTest {

    private static final ProjectChunks CHUNKS = TestChunks.example();
    private static final GitHubHead BRANCH = TestGitHubHead.example();
    private static final String WORKFLOW = "test-workflow.yaml";
    private final InMemoryGitHubWorkflowRuns workflowRuns = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);

    private CheckGitHubStatusConfig.Builder configurationBuilder() {
        return CheckGitHubStatusConfig.builder()
                .token("test-token").head(BRANCH)
                .chunks(CHUNKS);
    }

    @Test
    public void shouldBuildAllChunksWhenNoWorkflowRunsYet() throws Exception {
        // When
        WorkflowStatus status = configurationBuilder().build().checkStatusSingleWorkflow(workflowRuns, WORKFLOW);

        // Then
        assertThat(status.hasPreviousFailures()).isFalse();
        assertThat(status.previousBuildsReportLines()).containsExactly("",
                "Bulk Import: null",
                "",
                "Common: null",
                "",
                "Ingest: null");
        assertThat(status.chunkIdsToBuild()).containsExactly("bulk-import", "common", "ingest");
    }

    @Test
    public void shouldBuildNoChunksWhenNoChangesSinceLastRun() throws Exception {
        // Note that this assumes the previous build built all the chunks.
        // This will need to change when we add a way to tell which chunks were built without separate workflows.

        // Given
        workflowRuns.setLatestRun(GitHubWorkflowRun.builder()
                .pathsChangedSinceThisRunArray(".github/workflows/build.yaml")
                .success());

        // When
        WorkflowStatus status = configurationBuilder().build().checkStatusSingleWorkflow(workflowRuns, WORKFLOW);

        // Then
        assertThat(status.hasPreviousFailures()).isFalse();
        assertThat(status.previousBuildsReportLines()).containsExactly("",
                "Bulk Import: completed, success",
                "",
                "Common: completed, success",
                "",
                "Ingest: completed, success");
        assertThat(status.chunkIdsToBuild()).isEmpty();
    }
}
