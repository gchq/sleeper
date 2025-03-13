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
package sleeper.build.github;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryGitHubWorkflowRunsTest {
    private static final GitHubHead BRANCH = TestGitHubHead.example();
    private static final String WORKFLOW = "test-workflow";

    @Test
    public void shouldReturnSpecifiedRunAndRecheck() {
        // Given
        InMemoryGitHubWorkflowRuns runs = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);
        long runId = 123;
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("some-sha").runId(runId);

        runs.setLatestRunAndRechecks(runBuilder.inProgress(), runBuilder.success());

        // When / Then
        assertThat(runs.getLatestRun(BRANCH, WORKFLOW)).contains(runBuilder.inProgress());
        assertThat(runs.recheckRun(BRANCH, runId)).isEqualTo(runBuilder.success());
    }

    @Test
    public void shouldFailWhenUnspecifiedRecheckMade() {
        // Given
        InMemoryGitHubWorkflowRuns runs = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);

        // When / Then
        assertThatThrownBy(() -> runs.recheckRun(BRANCH, 123L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldFailWhenTooManyRechecksMade() {
        // Given
        InMemoryGitHubWorkflowRuns runs = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);
        long runId = 123;
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("some-sha").runId(runId);

        runs.setLatestRunAndRechecks(runBuilder.inProgress(), runBuilder.success());
        runs.recheckRun(BRANCH, runId);

        // When / Then
        assertThatThrownBy(() -> runs.recheckRun(BRANCH, runId))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldFailWhenRunQueriedForWrongCommit() {
        // Given
        InMemoryGitHubWorkflowRuns runs = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);
        GitHubHead queryHead = TestGitHubHead.exampleBuilder().sha("other-sha").build();
        runs.setLatestRun(GitHubWorkflowRun.withCommitSha("some-sha").runId(123L).inProgress());

        // When / Then
        assertThatThrownBy(() -> runs.getLatestRun(queryHead, WORKFLOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("head");
    }

    @Test
    public void shouldFailWhenRunRecheckedForWrongCommit() {
        // Given
        InMemoryGitHubWorkflowRuns runs = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);
        long runId = 123;
        GitHubHead queryHead = TestGitHubHead.exampleBuilder().sha("other-sha").build();
        GitHubWorkflowRun run = GitHubWorkflowRun.withCommitSha("some-sha").runId(runId).inProgress();
        runs.setLatestRunAndRecheck(run, run);

        // When / Then
        assertThatThrownBy(() -> runs.recheckRun(queryHead, runId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("head");
    }

    @Test
    public void shouldFailWhenRunQueriedForWrongWorkflow() {
        // Given
        InMemoryGitHubWorkflowRuns runs = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);
        runs.setLatestRun(GitHubWorkflowRun.withCommitSha("some-sha").runId(123L).inProgress());

        // When / Then
        assertThatThrownBy(() -> runs.getLatestRun(BRANCH, "other-workflow"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("workflow");
    }
}
