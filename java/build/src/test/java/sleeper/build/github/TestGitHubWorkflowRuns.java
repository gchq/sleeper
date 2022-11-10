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

import sleeper.build.chunks.ProjectConfiguration;
import sleeper.build.status.ChunksStatus;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGitHubWorkflowRuns {

    private final GitHubWorkflowRuns mock = mock(GitHubWorkflowRuns.class);

    public void setLatestRun(
            GitHubHead branch, String workflow, GitHubWorkflowRun run) {
        setLatestRunAndRecheck(branch, workflow, run);
    }

    public void setLatestRunAndRecheck(
            GitHubHead branch, String workflow, GitHubWorkflowRun run) {
        setLatestRunAndRecheck(branch, workflow, run, run);
    }

    public void setLatestRunAndRecheck(
            GitHubHead branch, String workflow, GitHubWorkflowRun run,
            GitHubWorkflowRun recheckRun, GitHubWorkflowRun... otherRechecks) {
        when(mock.getLatestRun(branch, workflow)).thenReturn(Optional.of(run));
        when(mock.recheckRun(branch, run.getRunId())).thenReturn(recheckRun, otherRechecks);
    }

    public ChunksStatus checkStatus(ProjectConfiguration configuration) {
        return configuration.checkStatus(mock);
    }

    public void verifyTimesRechecked(GitHubHead branch, GitHubWorkflowRun run, int times) {
        verify(mock, times(times)).recheckRun(branch, run.getRunId());
    }
}
