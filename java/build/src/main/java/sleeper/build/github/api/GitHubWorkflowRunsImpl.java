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
package sleeper.build.github.api;

import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubRunToHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.GitHubWorkflowRuns;

import javax.ws.rs.client.WebTarget;

import java.util.Objects;
import java.util.stream.Stream;

public class GitHubWorkflowRunsImpl implements GitHubWorkflowRuns, AutoCloseable {

    private final GitHubApi api;

    public GitHubWorkflowRunsImpl(String token) {
        this(GitHubApi.withToken(token));
    }

    public GitHubWorkflowRunsImpl(GitHubApi api) {
        this.api = api;
    }

    @Override
    public Stream<GitHubWorkflowRun> getRunsForHeadOrBehindLatestFirst(GitHubHead head, String workflow) {
        WebTarget repository = repository(head);
        WebTarget runs = repository.path("actions/workflows").path(workflow)
                .path("runs").queryParam("branch", head.getBranch());
        GitHubWorkflowRunsResponse response = api.request(runs).get(GitHubWorkflowRunsResponse.class);
        return response.getWorkflowRuns().stream()
                .map(GitHubWorkflowRunsResponse.Run::toInternalRun)
                .map(run -> compare(repository, run, head))
                .filter(GitHubRunToHead::isRunForHeadOrBehind)
                .map(GitHubRunToHead::getRun);
    }

    @Override
    public GitHubWorkflowRun recheckRun(GitHubHead head, Long runId) {
        WebTarget repository = repository(head);
        WebTarget run = repository.path("actions/runs").path("" + runId);
        GitHubWorkflowRunsResponse.Run response = api.request(run).get(GitHubWorkflowRunsResponse.Run.class);
        return response.toInternalRun();
    }

    private GitHubRunToHead compare(
            WebTarget repository, GitHubWorkflowRun run, GitHubHead head) {
        if (Objects.equals(run.getCommitSha(), head.getSha())) {
            return GitHubRunToHead.sameSha(run);
        }
        WebTarget compare = repository.path("compare").path(run.getCommitSha() + "..." + head.getSha());
        GitHubCompareResponse response = api.request(compare).get(GitHubCompareResponse.class);
        return response.toRunToHead(run);
    }

    private WebTarget repository(GitHubHead head) {
        return api.path("repos").path(head.getOwnerAndRepository());
    }

    @Override
    public void close() {
        api.close();
    }
}
