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

import sleeper.build.status.CheckGitHubStatusConfig;
import sleeper.build.status.ChunkStatuses;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryGitHubWorkflowRuns implements GitHubWorkflowRuns {

    private final GitHubHead head;
    private final String workflow;
    private final List<GitHubWorkflowRun> runsLatestFirst = new ArrayList<>();
    private final Map<Long, Queue<GitHubWorkflowRun>> recheckResultsByRunId = new HashMap<>();

    public InMemoryGitHubWorkflowRuns(GitHubHead head, String workflow) {
        this.head = head;
        this.workflow = workflow;
    }

    @Override
    public Stream<GitHubWorkflowRun> getRunsForHeadOrBehindLatestFirst(GitHubHead head, String workflow) {
        validateHead(head);
        validateWorkflow(workflow);
        return runsLatestFirst.stream();
    }

    @Override
    public GitHubWorkflowRun recheckRun(GitHubHead head, Long runId) {
        validateHead(head);
        Queue<GitHubWorkflowRun> resultsQueue = recheckResultsByRunId.get(runId);
        if (resultsQueue == null) {
            throw new IllegalStateException("Unexpected recheck for run " + runId);
        }
        GitHubWorkflowRun run = resultsQueue.poll();
        if (resultsQueue.isEmpty()) {
            recheckResultsByRunId.remove(runId);
        }
        return run;
    }

    private void validateHead(GitHubHead head) {
        if (!this.head.equals(head)) {
            throw new IllegalStateException("Found unexpected Git head: " + head);
        }
    }

    private void validateWorkflow(String workflow) {
        if (!this.workflow.equals(workflow)) {
            throw new IllegalStateException("Found unexpected workflow: " + workflow);
        }
    }

    public void setLatestRun(GitHubWorkflowRun run) {
        runsLatestFirst.clear();
        runsLatestFirst.add(run);
    }

    public void setRunsLatestFirst(GitHubWorkflowRun... runs) {
        runsLatestFirst.clear();
        runsLatestFirst.addAll(Arrays.asList(runs));
    }

    public void setLatestRunAndRecheck(GitHubWorkflowRun run, GitHubWorkflowRun recheck) {
        setLatestRunAndRechecks(run, recheck);
    }

    public void setLatestRunAndRechecks(GitHubWorkflowRun run, GitHubWorkflowRun... rechecks) {
        setLatestRunAndRechecks(run, Arrays.asList(rechecks));
    }

    public void setLatestRunAndRechecks(GitHubWorkflowRun run, List<GitHubWorkflowRun> rechecks) {
        setLatestRun(run);
        recheckResultsByRunId.put(run.getRunId(), new LinkedList<>(rechecks));
    }

    public ChunkStatuses checkStatus(CheckGitHubStatusConfig configuration) {
        return configuration.checkStatus(this);
    }

    public void verifyExactlySpecifiedRechecksDone() {
        assertThat(recheckResultsByRunId).isEmpty();
    }
}
