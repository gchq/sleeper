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
package sleeper.build.github;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGitHubWorkflowRunsTest {
    private static final GitHubHead BRANCH = GitHubHead.builder()
            .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
            .build();
    private static final String WORKFLOW = "test-workflow";

    @Test
    public void shouldReturnSpecifiedRunAndRecheck() {
        // Given
        TestGitHubWorkflowRuns runs = new TestGitHubWorkflowRuns(BRANCH, WORKFLOW);
        long runId = 123;
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("test-sha").runId(runId);

        runs.setLatestRunAndRechecks(runBuilder.inProgress(), runBuilder.success());

        // When / Then
        assertThat(runs.getLatestRun(BRANCH, WORKFLOW)).contains(runBuilder.inProgress());
        assertThat(runs.recheckRun(BRANCH, runId)).isEqualTo(runBuilder.success());
    }

    @Test
    public void shouldFailWhenUnspecifiedRecheckMade() {
        // Given
        TestGitHubWorkflowRuns runs = new TestGitHubWorkflowRuns(BRANCH, WORKFLOW);

        // When / Then
        assertThatThrownBy(() -> runs.recheckRun(BRANCH, 123L))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldFailWhenTooManyRechecksMade() {
        // Given
        TestGitHubWorkflowRuns runs = new TestGitHubWorkflowRuns(BRANCH, WORKFLOW);
        long runId = 123;
        GitHubWorkflowRun.Builder runBuilder = GitHubWorkflowRun.withCommitSha("test-sha").runId(runId);

        runs.setLatestRunAndRechecks(runBuilder.inProgress(), runBuilder.success());
        runs.recheckRun(BRANCH, runId);

        // When / Then
        assertThatThrownBy(() -> runs.recheckRun(BRANCH, runId))
                .isInstanceOf(IllegalStateException.class);
    }
}
